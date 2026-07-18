package textract

import (
	"errors"
	"fmt"
	"image"
	"sort"

	"github.com/otiai10/gosseract/v2"
	"gocv.io/x/gocv"
)

// DecodedImage wraps a decoded gocv.Mat along with its pixel dimensions.
// Callers must call Close() to release the underlying C memory.
type DecodedImage struct {
	Mat    *gocv.Mat
	Width  int
	Height int
}

// Close releases the underlying OpenCV Mat.
func (d *DecodedImage) Close() {
	if d.Mat != nil {
		_ = d.Mat.Close()
	}
}

// DecodeImage decodes raw raster image bytes (as sent in a Textract
// Document.Bytes payload) into an OpenCV Mat. PDFs and other non-raster
// formats are not supported and surface as a decode failure here.
func DecodeImage(raw []byte) (*DecodedImage, error) {
	mat, err := gocv.IMDecode(raw, gocv.IMReadColor)
	if err != nil {
		return nil, fmt.Errorf("decode image: %w", err)
	}
	if mat.Empty() {
		_ = mat.Close()
		return nil, errors.New("decode image: empty result (unsupported or corrupt image)")
	}
	return &DecodedImage{Mat: &mat, Width: mat.Cols(), Height: mat.Rows()}, nil
}

// Preprocess converts src to grayscale and applies Otsu thresholding,
// producing a binary image suited to both Tesseract OCR and the
// morphological table/selection-element heuristics below. The returned Mat
// is newly allocated; callers must Close() it.
func Preprocess(src *gocv.Mat) (*gocv.Mat, error) {
	gray := gocv.NewMat()
	defer gray.Close()
	gocv.CvtColor(*src, &gray, gocv.ColorBGRToGray)

	bin := gocv.NewMat()
	gocv.Threshold(gray, &bin, 0, 255, gocv.ThresholdBinary+gocv.ThresholdOtsu)
	return &bin, nil
}

// EncodeToBytes re-encodes m as a PNG byte buffer, e.g. to feed a
// preprocessed Mat back into Tesseract via SetImageFromBytes.
func EncodeToBytes(m *gocv.Mat) ([]byte, error) {
	buf, err := gocv.IMEncode(gocv.PNGFileExt, *m)
	if err != nil {
		return nil, fmt.Errorf("encode image: %w", err)
	}
	defer buf.Close()
	return buf.GetBytes(), nil
}

// OCRWord is a single OCR'd word with its pixel bounding box and confidence.
type OCRWord struct {
	Text       string
	Box        image.Rectangle
	Confidence float64
}

// OCRLine groups the words Tesseract assigned to the same text line.
type OCRLine struct {
	Text       string
	Box        image.Rectangle
	Confidence float64
	Words      []OCRWord
}

// RunOCR extracts line- and word-level text, bounding boxes, and confidence
// scores from imgBytes using Tesseract. A fresh gosseract.Client is created
// per call since gosseract clients are not safe for concurrent reuse
// across requests.
func RunOCR(imgBytes []byte, langs ...string) ([]OCRLine, error) {
	if len(langs) == 0 {
		langs = []string{"eng"}
	}

	client := gosseract.NewClient()
	defer client.Close()

	if err := client.SetLanguage(langs...); err != nil {
		return nil, fmt.Errorf("ocr: set language: %w", err)
	}
	if err := client.SetImageFromBytes(imgBytes); err != nil {
		return nil, fmt.Errorf("ocr: set image: %w", err)
	}

	lineBoxes, err := client.GetBoundingBoxes(gosseract.RIL_TEXTLINE)
	if err != nil {
		return nil, fmt.Errorf("ocr: line bounding boxes: %w", err)
	}
	wordBoxes, err := client.GetBoundingBoxes(gosseract.RIL_WORD)
	if err != nil {
		return nil, fmt.Errorf("ocr: word bounding boxes: %w", err)
	}

	return assembleLines(lineBoxes, wordBoxes), nil
}

// lineKey identifies the line a word/line-box belongs to within
// Tesseract's block/paragraph/line hierarchy.
type lineKey struct{ block, par, line int }

// assembleLines groups Tesseract's flat word-box list under their parent
// line-box, using the BlockNum/ParNum/LineNum triple both levels share.
func assembleLines(lineBoxes, wordBoxes []gosseract.BoundingBox) []OCRLine {
	order := make([]lineKey, 0, len(lineBoxes))
	byKey := make(map[lineKey]*OCRLine, len(lineBoxes))

	for _, lb := range lineBoxes {
		key := lineKey{lb.BlockNum, lb.ParNum, lb.LineNum}
		if _, exists := byKey[key]; exists {
			continue
		}
		byKey[key] = &OCRLine{Text: lb.Word, Box: lb.Box, Confidence: lb.Confidence}
		order = append(order, key)
	}

	for _, wb := range wordBoxes {
		key := lineKey{wb.BlockNum, wb.ParNum, wb.LineNum}
		line, ok := byKey[key]
		if !ok {
			// Word belongs to a line Tesseract didn't report at
			// RIL_TEXTLINE (rare) — synthesize one so it isn't dropped.
			line = &OCRLine{Text: wb.Word, Box: wb.Box, Confidence: wb.Confidence}
			byKey[key] = line
			order = append(order, key)
		}
		line.Words = append(line.Words, OCRWord{Text: wb.Word, Box: wb.Box, Confidence: wb.Confidence})
	}

	lines := make([]OCRLine, 0, len(order))
	for _, key := range order {
		lines = append(lines, *byKey[key])
	}
	return lines
}

// --- table / form-field layout heuristics ---
//
// These use standard OpenCV morphology recipes (elongated structuring
// elements to isolate long straight rules) rather than any ML layout
// model. They approximate real Textract's TABLES/FORMS feature output
// well enough to exercise the JSON shape, not to match its accuracy.

// DetectTableGrids finds candidate table regions in a binary (thresholded)
// image: isolate long horizontal and vertical rules via morphological
// erosion+dilation with elongated structuring elements, OR the two masks
// together, then treat each resulting contour's bounding box as a table.
func DetectTableGrids(bin *gocv.Mat) []image.Rectangle {
	inv := gocv.NewMat()
	defer inv.Close()
	gocv.BitwiseNot(*bin, &inv)

	horizontal := extractLines(&inv, bin.Cols()/30, 1)
	defer horizontal.Close()
	vertical := extractLines(&inv, 1, bin.Rows()/30)
	defer vertical.Close()

	mask := gocv.NewMat()
	defer mask.Close()
	gocv.BitwiseOr(horizontal, vertical, &mask)

	contours := gocv.FindContours(mask, gocv.RetrievalExternal, gocv.ChainApproxSimple)
	defer contours.Close()

	minArea := (bin.Cols() * bin.Rows()) / 400 // ignore tiny noise contours
	var rects []image.Rectangle
	for i := 0; i < contours.Size(); i++ {
		rect := gocv.BoundingRect(contours.At(i))
		if rect.Dx()*rect.Dy() >= minArea {
			rects = append(rects, rect)
		}
	}
	return rects
}

// extractLines isolates long straight lines from a binary (white-on-black)
// image using an elongated structuring element of the given size.
func extractLines(bin *gocv.Mat, kw, kh int) gocv.Mat {
	if kw < 1 {
		kw = 1
	}
	if kh < 1 {
		kh = 1
	}
	kernel := gocv.GetStructuringElement(gocv.MorphRect, image.Pt(kw, kh))
	defer kernel.Close()

	out := gocv.NewMat()
	gocv.Erode(*bin, &out, kernel)
	gocv.Dilate(out, &out, kernel)
	return out
}

// DetectCellGrids subdivides a table region into a grid of cell
// rectangles by locating internal horizontal and vertical rule positions.
// Cells are returned as rows of columns (top-to-bottom, left-to-right),
// matching the RowIndex/ColumnIndex order Textract expects. If no internal
// grid lines are found, the whole table is returned as a single cell.
func DetectCellGrids(bin *gocv.Mat, table image.Rectangle) [][]image.Rectangle {
	region := bin.Region(table)
	defer region.Close()

	rowBounds := lineOffsets(&region, region.Rows()/40, 1, true)
	colBounds := lineOffsets(&region, 1, region.Cols()/40, false)

	if len(rowBounds) < 2 || len(colBounds) < 2 {
		return [][]image.Rectangle{{table}}
	}

	grid := make([][]image.Rectangle, 0, len(rowBounds)-1)
	for r := 0; r < len(rowBounds)-1; r++ {
		row := make([]image.Rectangle, 0, len(colBounds)-1)
		for c := 0; c < len(colBounds)-1; c++ {
			row = append(row, image.Rect(
				table.Min.X+colBounds[c], table.Min.Y+rowBounds[r],
				table.Min.X+colBounds[c+1], table.Min.Y+rowBounds[r+1],
			))
		}
		grid = append(grid, row)
	}
	return grid
}

// lineOffsets finds the sorted, de-duplicated pixel offsets of horizontal
// (horizontal=true) or vertical rule lines within region, including the
// region's own start/end bounds.
func lineOffsets(region *gocv.Mat, kw, kh int, horizontal bool) []int {
	lines := extractLines(region, kw, kh)
	defer lines.Close()

	contours := gocv.FindContours(lines, gocv.RetrievalExternal, gocv.ChainApproxSimple)
	defer contours.Close()

	limit := region.Cols()
	if horizontal {
		limit = region.Rows()
	}

	offsets := []int{0, limit}
	for i := 0; i < contours.Size(); i++ {
		rect := gocv.BoundingRect(contours.At(i))
		if horizontal {
			offsets = append(offsets, rect.Min.Y+rect.Dy()/2)
		} else {
			offsets = append(offsets, rect.Min.X+rect.Dx()/2)
		}
	}

	sort.Ints(offsets)
	return dedupeClose(offsets, limit/50+1)
}

// dedupeClose collapses consecutive values in sorted that are within
// minGap of each other, keeping the first of each cluster.
func dedupeClose(sorted []int, minGap int) []int {
	if len(sorted) == 0 {
		return sorted
	}
	out := []int{sorted[0]}
	for _, v := range sorted[1:] {
		if v-out[len(out)-1] >= minGap {
			out = append(out, v)
		}
	}
	return out
}

// SelectionCandidate is a detected checkbox/radio-button-shaped region and
// whether it appears filled in.
type SelectionCandidate struct {
	Box      image.Rectangle
	Selected bool
}

// DetectSelectionElements finds small near-square contours outside any
// already-detected table region and classifies each as selected/unselected
// based on the fraction of foreground (dark) pixels inside it — a rough
// proxy for a filled checkbox/radio button.
func DetectSelectionElements(bin *gocv.Mat, exclude []image.Rectangle) []SelectionCandidate {
	inv := gocv.NewMat()
	defer inv.Close()
	gocv.BitwiseNot(*bin, &inv)

	contours := gocv.FindContours(inv, gocv.RetrievalList, gocv.ChainApproxSimple)
	defer contours.Close()

	var out []SelectionCandidate
	for i := 0; i < contours.Size(); i++ {
		rect := gocv.BoundingRect(contours.At(i))
		if !looksLikeSelectionBox(rect) || overlapsAny(rect, exclude) {
			continue
		}
		region := inv.Region(rect)
		filled := float64(gocv.CountNonZero(region)) / float64(rect.Dx()*rect.Dy())
		region.Close()
		out = append(out, SelectionCandidate{Box: rect, Selected: filled > 0.35})
	}
	return out
}

func looksLikeSelectionBox(r image.Rectangle) bool {
	w, h := r.Dx(), r.Dy()
	if w < 8 || h < 8 || w > 60 || h > 60 {
		return false
	}
	ratio := float64(w) / float64(h)
	return ratio > 0.75 && ratio < 1.35
}

func overlapsAny(r image.Rectangle, rects []image.Rectangle) bool {
	for _, other := range rects {
		if r.Overlaps(other) {
			return true
		}
	}
	return false
}
