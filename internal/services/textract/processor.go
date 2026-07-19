// Package textract emulates the AWS Textract DetectDocumentText and
// AnalyzeDocument APIs using real OpenCV (gocv) preprocessing and real
// Tesseract (gosseract) OCR.
//
// FORMS and TABLES detection are heuristic approximations, not an ML-grade
// layout model: TABLES uses morphological line detection to find table
// grids, and FORMS anchors each key to a real detected underline/rule
// first, falling back to spatial proximity when no drawn line is found.
// The goal is to faithfully reproduce the JSON Block-tree *shape* AWS SDKs
// expect, not pixel-perfect parity with Textract's real accuracy.
package textract

import (
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"strings"

	"github.com/google/uuid"
	"gocv.io/x/gocv"
)

// structuralConfidence is the fixed confidence assigned to blocks with no
// natural OCR-derived score (PAGE, TABLE, CELL, KEY_VALUE_SET) — OpenCV
// contour detection has no natural probabilistic confidence of its own.
const structuralConfidence = 90.0

var (
	// ErrUnsupportedDocument means the input could not be decoded as a
	// raster image (includes PDFs, since gocv.IMDecode is raster-only).
	ErrUnsupportedDocument = errors.New("document could not be decoded as an image")
	// ErrInvalidParameter mirrors Textract's InvalidParameterException.
	ErrInvalidParameter = errors.New("invalid parameter")
)

// --- Textract JSON Block shape ---

type BoundingBox struct {
	Width  float64 `json:"Width"`
	Height float64 `json:"Height"`
	Left   float64 `json:"Left"`
	Top    float64 `json:"Top"`
}

type Point struct {
	X float64 `json:"X"`
	Y float64 `json:"Y"`
}

type Geometry struct {
	BoundingBox   BoundingBox `json:"BoundingBox"`
	Polygon       []Point     `json:"Polygon"`
	RotationAngle float64     `json:"RotationAngle"`
}

type Relationship struct {
	Type string   `json:"Type"`
	Ids  []string `json:"Ids"`
}

type Block struct {
	BlockType       string         `json:"BlockType"`
	Id              string         `json:"Id"`
	Confidence      float64        `json:"Confidence,omitempty"`
	Text            string         `json:"Text,omitempty"`
	TextType        string         `json:"TextType,omitempty"`
	RowIndex        int            `json:"RowIndex,omitempty"`
	ColumnIndex     int            `json:"ColumnIndex,omitempty"`
	RowSpan         int            `json:"RowSpan,omitempty"`
	ColumnSpan      int            `json:"ColumnSpan,omitempty"`
	EntityTypes     []string       `json:"EntityTypes,omitempty"`
	SelectionStatus string         `json:"SelectionStatus,omitempty"`
	Geometry        Geometry       `json:"Geometry"`
	Relationships   []Relationship `json:"Relationships,omitempty"`
	Page            int            `json:"Page"`
}

// MarshalJSON emits Text (even when empty) for WORD/LINE blocks — the only
// types real Textract populates it for — and omits it entirely for every
// other BlockType, matching AWS's shape exactly. A plain `omitempty` tag on
// Text can't express this: it would drop Text for a WORD/LINE with no
// recognized characters too, which broke SDK code that indexes resp["Text"]
// unconditionally on every LINE block.
func (b Block) MarshalJSON() ([]byte, error) {
	type alias Block
	out := struct {
		alias
		Text *string `json:"Text,omitempty"`
	}{alias: alias(b)}
	if b.BlockType == "WORD" || b.BlockType == "LINE" {
		text := b.Text
		out.Text = &text
	}
	return json.Marshal(out)
}

type DocumentMetadata struct {
	Pages int `json:"Pages"`
}

// Warning mirrors Textract's Warning shape, surfaced in async Get*
// responses when a page couldn't be fully processed. This emulator never
// produces partial failures, so callers always see an empty/omitted list.
type Warning struct {
	ErrorCode string `json:"ErrorCode"`
	Pages     []int  `json:"Pages,omitempty"`
}

// modelVersion is a fixed placeholder for the *ModelVersion response
// fields real Textract populates with its current model identifier.
const modelVersion = "1.0"

type DetectDocumentTextOutput struct {
	DocumentMetadata               DocumentMetadata `json:"DocumentMetadata"`
	DetectDocumentTextModelVersion string           `json:"DetectDocumentTextModelVersion"`
	Blocks                         []Block          `json:"Blocks"`
}

type AnalyzeDocumentOutput struct {
	DocumentMetadata            DocumentMetadata `json:"DocumentMetadata"`
	AnalyzeDocumentModelVersion string           `json:"AnalyzeDocumentModelVersion"`
	Blocks                      []Block          `json:"Blocks"`
}

// Processor turns raw document bytes into Textract-shaped Block trees. The
// only state it carries is the in-memory async job store used by
// StartDocumentTextDetection/StartDocumentAnalysis.
type Processor struct {
	jobs *jobStore
}

// NewProcessor returns a Textract processor with an empty async job store.
func NewProcessor() *Processor {
	return &Processor{jobs: newJobStore()}
}

// --- block-tree builder ---

// builder accumulates Blocks and converts pixel-space rectangles into
// Textract's fractional (image-relative) Geometry.
type builder struct {
	width, height float64
	blocks        []Block
}

func newBuilder(width, height int) *builder {
	return &builder{width: float64(width), height: float64(height)}
}

func (b *builder) geometry(box image.Rectangle) Geometry {
	w, h := b.width, b.height
	left := float64(box.Min.X) / w
	top := float64(box.Min.Y) / h
	width := float64(box.Dx()) / w
	height := float64(box.Dy()) / h
	return Geometry{
		BoundingBox: BoundingBox{Width: width, Height: height, Left: left, Top: top},
		Polygon: []Point{
			{X: left, Y: top},
			{X: left + width, Y: top},
			{X: left + width, Y: top + height},
			{X: left, Y: top + height},
		},
	}
}

// add mints an Id (if unset), stamps Page, appends the block, and returns
// its Id for parent blocks to reference via Relationships.
func (b *builder) add(block Block) string {
	if block.Id == "" {
		block.Id = uuid.NewString()
	}
	block.Page = 1
	b.blocks = append(b.blocks, block)
	return block.Id
}

// placedWord/placedLine carry a block's already-minted Id alongside its
// pixel-space box, so downstream FORMS/TABLES augmentation can reference
// the exact same WORD blocks emitted by the base OCR pass instead of
// re-deriving them.
type placedWord struct {
	ID   string
	Box  image.Rectangle
	Text string
}

type placedLine struct {
	ID         string
	Box        image.Rectangle
	Text       string
	Confidence float64
	WordIDs    []string
}

// buildTextBlocks runs the shared pipeline (decode -> preprocess ->
// re-encode -> Tesseract) and emits PAGE/LINE/WORD blocks. It returns the
// builder for further augmentation, the placed lines/words for FORMS/
// TABLES heuristics, and the binary Mat used for layout detection —
// callers must Close() the returned *gocv.Mat.
func (p *Processor) buildTextBlocks(raw []byte) (*builder, []placedLine, []placedWord, *gocv.Mat, error) {
	decoded, err := DecodeImage(raw)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("%w: %v", ErrUnsupportedDocument, err)
	}
	defer decoded.Close()

	bin, err := Preprocess(decoded.Mat)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("preprocess: %w", err)
	}

	ocrInput, err := EncodeToBytes(bin)
	if err != nil {
		bin.Close()
		return nil, nil, nil, nil, fmt.Errorf("encode for ocr: %w", err)
	}

	ocrLines, err := RunOCR(ocrInput)
	if err != nil {
		bin.Close()
		return nil, nil, nil, nil, fmt.Errorf("ocr: %w", err)
	}

	b := newBuilder(decoded.Width, decoded.Height)

	var lines []placedLine
	var words []placedWord
	lineIDs := make([]string, 0, len(ocrLines))

	for _, line := range ocrLines {
		wordIDs := make([]string, 0, len(line.Words))
		for _, word := range line.Words {
			wid := b.add(Block{
				BlockType:  "WORD",
				Confidence: word.Confidence,
				Text:       word.Text,
				TextType:   "PRINTED",
				Geometry:   b.geometry(word.Box),
			})
			wordIDs = append(wordIDs, wid)
			words = append(words, placedWord{ID: wid, Box: word.Box, Text: word.Text})
		}
		lineBlock := Block{
			BlockType:     "LINE",
			Confidence:    line.Confidence,
			Text:          line.Text,
			Geometry:      b.geometry(line.Box),
			Relationships: relIfAny("CHILD", wordIDs),
		}
		lid := b.add(lineBlock)
		lineIDs = append(lineIDs, lid)
		lines = append(lines, placedLine{ID: lid, Box: line.Box, Text: line.Text, Confidence: line.Confidence, WordIDs: wordIDs})
	}

	pageBlock := Block{
		BlockType:     "PAGE",
		Id:            uuid.NewString(),
		Page:          1,
		Geometry:      b.geometry(image.Rect(0, 0, decoded.Width, decoded.Height)),
		Relationships: relIfAny("CHILD", lineIDs),
	}
	// Prepend PAGE so it appears first, matching real Textract's block
	// ordering (added directly rather than via b.add, which appends).
	b.blocks = append([]Block{pageBlock}, b.blocks...)

	return b, lines, words, bin, nil
}

// DetectDocumentText emulates Textract's DetectDocumentText: OCR only, no
// FORMS/TABLES analysis.
func (p *Processor) DetectDocumentText(raw []byte) (*DetectDocumentTextOutput, error) {
	b, _, _, bin, err := p.buildTextBlocks(raw)
	if err != nil {
		return nil, err
	}
	bin.Close()

	return &DetectDocumentTextOutput{
		DocumentMetadata:               DocumentMetadata{Pages: 1},
		DetectDocumentTextModelVersion: modelVersion,
		Blocks:                         b.blocks,
	}, nil
}

var validFeatureTypes = map[string]bool{"FORMS": true, "TABLES": true}

// AnalyzeDocument emulates Textract's AnalyzeDocument, additionally
// running the FORMS and/or TABLES heuristics requested in featureTypes.
func (p *Processor) AnalyzeDocument(raw []byte, featureTypes []string) (*AnalyzeDocumentOutput, error) {
	if len(featureTypes) == 0 {
		return nil, fmt.Errorf("%w: FeatureTypes must not be empty", ErrInvalidParameter)
	}
	wantForms, wantTables := false, false
	for _, ft := range featureTypes {
		if !validFeatureTypes[ft] {
			return nil, fmt.Errorf("%w: unsupported FeatureType %q", ErrInvalidParameter, ft)
		}
		if ft == "FORMS" {
			wantForms = true
		}
		if ft == "TABLES" {
			wantTables = true
		}
	}

	b, lines, words, bin, err := p.buildTextBlocks(raw)
	if err != nil {
		return nil, err
	}
	defer bin.Close()

	var tableRects []image.Rectangle
	if wantTables {
		tableRects = DetectTableGrids(bin)
		p.addTables(b, bin, tableRects, words)
	}
	if wantForms {
		underlines := DetectFieldUnderlines(bin, tableRects)
		p.addKeyValueSets(b, lines, tableRects, underlines)
		p.addSelectionElements(b, bin, tableRects)
	}

	return &AnalyzeDocumentOutput{
		DocumentMetadata:            DocumentMetadata{Pages: 1},
		AnalyzeDocumentModelVersion: modelVersion,
		Blocks:                      b.blocks,
	}, nil
}

// addTables emits TABLE and CELL blocks for each detected table region,
// assigning each already-OCR'd word to the cell whose rectangle contains
// the word's center point. CELL blocks carry no direct Text field —
// matching real Textract, where cell text is derived by walking CHILD
// word ids.
func (p *Processor) addTables(b *builder, bin *gocv.Mat, tables []image.Rectangle, words []placedWord) {
	for _, table := range tables {
		grid := DetectCellGrids(bin, table)
		var cellIDs []string
		for r, row := range grid {
			for c, cellBox := range row {
				var wordIDsInCell []string
				for _, word := range words {
					center := image.Pt(word.Box.Min.X+word.Box.Dx()/2, word.Box.Min.Y+word.Box.Dy()/2)
					if center.In(cellBox) {
						wordIDsInCell = append(wordIDsInCell, word.ID)
					}
				}
				cellIDs = append(cellIDs, b.add(Block{
					BlockType:     "CELL",
					Confidence:    structuralConfidence,
					RowIndex:      r + 1,
					ColumnIndex:   c + 1,
					RowSpan:       1,
					ColumnSpan:    1, // no merged-cell detection — documented simplification
					Geometry:      b.geometry(cellBox),
					Relationships: relIfAny("CHILD", wordIDsInCell),
				}))
			}
		}
		b.add(Block{
			BlockType:     "TABLE",
			Confidence:    structuralConfidence,
			Geometry:      b.geometry(table),
			Relationships: relIfAny("CHILD", cellIDs),
		})
	}
}

// formValue is a form field's value: either an OCR line bound to a key —
// via its underline anchor or, failing that, layout proximity — or, when a
// detected underline has no OCR text over its writable area, an empty
// value box. Real Textract emits a VALUE block even for a blank form
// field, so an empty formValue still produces one.
type formValue struct {
	Box        image.Rectangle
	Text       string
	Confidence float64
	WordIDs    []string
	lineID     string // backing placedLine.ID, if any; claimed to prevent reuse.
}

// kvPair is a matched key/value pair, as found by pairKeyValues.
type kvPair struct {
	Key   placedLine
	Value formValue
}

// pairKeyValues pairs lines into key/value fields. A line ending in ':'
// (or short text, <=4 words) is treated as a KEY candidate. Its value is
// found in two passes:
//  1. Anchor to a real drawn underline/rule (the same morphological line
//     detection TABLES uses): the nearest unclaimed underline to the
//     key's right in the same horizontal band, or failing that, directly
//     below with a similar left edge. Whatever OCR line overlaps the
//     writable area just above that underline becomes the value; if none
//     does, the field is still emitted with an empty value, matching a
//     blank form field.
//  2. Falling back to pure layout proximity (nearest unclaimed line to
//     the right, then below) when no underline anchor was found.
//
// Shared by the FORMS feature (addKeyValueSets) and AnalyzeExpense, which
// both need the same key/value layout heuristic but emit it in different
// output shapes.
func pairKeyValues(lines []placedLine, tableRegions []image.Rectangle, underlines []image.Rectangle) []kvPair {
	claimed := make(map[string]bool, len(lines))
	claimedUnderlines := make([]bool, len(underlines))

	inTable := func(box image.Rectangle) bool {
		for _, t := range tableRegions {
			if box.Overlaps(t) {
				return true
			}
		}
		return false
	}

	var pairs []kvPair
	for _, key := range lines {
		if claimed[key.ID] || inTable(key.Box) || !looksLikeKey(key.Text) {
			continue
		}

		value := findValueViaUnderline(key, underlines, claimedUnderlines, lines, claimed, inTable)
		if value == nil {
			if v := findValueToRight(key, lines, claimed, inTable); v != nil {
				value = &formValue{Box: v.Box, Text: v.Text, Confidence: v.Confidence, WordIDs: v.WordIDs, lineID: v.ID}
			}
		}
		if value == nil {
			if v := findValueBelow(key, lines, claimed, inTable); v != nil {
				value = &formValue{Box: v.Box, Text: v.Text, Confidence: v.Confidence, WordIDs: v.WordIDs, lineID: v.ID}
			}
		}
		if value == nil {
			continue
		}

		claimed[key.ID] = true
		if value.lineID != "" {
			claimed[value.lineID] = true
		}
		pairs = append(pairs, kvPair{Key: key, Value: *value})
	}
	return pairs
}

// findValueViaUnderline anchors key's value to the nearest unclaimed
// detected underline (same search order as the proximity fallback: same
// band to the right, else directly below with a similar left edge), then
// binds whichever OCR line overlaps the writable area just above that
// underline. Returns nil if no underline anchor is found at all — a
// found-but-empty underline still yields a (blank) formValue, since the
// underline itself is real CV signal that a field exists there.
func findValueViaUnderline(key placedLine, underlines []image.Rectangle, claimedUnderlines []bool, lines []placedLine, claimedLines map[string]bool, inTable func(image.Rectangle) bool) *formValue {
	idx := nearestUnderlineToRight(key, underlines, claimedUnderlines)
	if idx < 0 {
		idx = nearestUnderlineBelow(key, underlines, claimedUnderlines)
	}
	if idx < 0 {
		return nil
	}
	claimedUnderlines[idx] = true
	underline := underlines[idx]

	lineHeight := key.Box.Dy()
	writable := image.Rect(underline.Min.X, underline.Min.Y-lineHeight-2, underline.Max.X, underline.Min.Y+2)

	for i := range lines {
		candidate := lines[i]
		if candidate.ID == key.ID || claimedLines[candidate.ID] || inTable(candidate.Box) {
			continue
		}
		if candidate.Box.Overlaps(writable) {
			return &formValue{Box: candidate.Box, Text: candidate.Text, Confidence: candidate.Confidence, WordIDs: candidate.WordIDs, lineID: candidate.ID}
		}
	}

	return &formValue{Box: underline, Confidence: structuralConfidence}
}

// nearestUnclaimedUnderline shares the two underline-search passes needed
// by findValueViaUnderline. within reports whether candidate underline u
// lies in the direction/band expected relative to key.
func nearestUnclaimedUnderline(key placedLine, underlines []image.Rectangle, claimed []bool, within func(u image.Rectangle) (ok bool, dist int)) int {
	best := -1
	bestDist := 0
	for i, u := range underlines {
		if claimed[i] {
			continue
		}
		ok, dist := within(u)
		if !ok {
			continue
		}
		if best < 0 || dist < bestDist {
			best, bestDist = i, dist
		}
	}
	return best
}

// nearestUnderlineToRight finds the nearest unclaimed underline to the
// right of key, within the same horizontal text band.
func nearestUnderlineToRight(key placedLine, underlines []image.Rectangle, claimed []bool) int {
	return nearestUnclaimedUnderline(key, underlines, claimed, func(u image.Rectangle) (bool, int) {
		if u.Min.X <= key.Box.Max.X || !sameBand(key.Box, u) {
			return false, 0
		}
		return true, u.Min.X - key.Box.Max.X
	})
}

// nearestUnderlineBelow finds the nearest unclaimed underline directly
// below key with a similar left edge.
func nearestUnderlineBelow(key placedLine, underlines []image.Rectangle, claimed []bool) int {
	return nearestUnclaimedUnderline(key, underlines, claimed, func(u image.Rectangle) (bool, int) {
		if u.Min.Y <= key.Box.Max.Y || abs(u.Min.X-key.Box.Min.X) > 40 {
			return false, 0
		}
		if dist := u.Min.Y - key.Box.Max.Y; dist <= key.Box.Dy()*4 {
			return true, dist
		}
		return false, 0
	})
}

// addKeyValueSets emits paired KEY_VALUE_SET blocks for each pairKeyValues
// match, reusing the same WORD ids as the underlying LINE blocks.
func (p *Processor) addKeyValueSets(b *builder, lines []placedLine, tableRegions []image.Rectangle, underlines []image.Rectangle) {
	for _, pair := range pairKeyValues(lines, tableRegions, underlines) {
		valueID := b.add(Block{
			BlockType:     "KEY_VALUE_SET",
			Confidence:    structuralConfidence,
			EntityTypes:   []string{"VALUE"},
			Geometry:      b.geometry(pair.Value.Box),
			Relationships: relIfAny("CHILD", pair.Value.WordIDs),
		})

		var keyRels []Relationship
		if len(pair.Key.WordIDs) > 0 {
			keyRels = append(keyRels, Relationship{Type: "CHILD", Ids: pair.Key.WordIDs})
		}
		keyRels = append(keyRels, Relationship{Type: "VALUE", Ids: []string{valueID}})

		b.add(Block{
			BlockType:     "KEY_VALUE_SET",
			Confidence:    structuralConfidence,
			EntityTypes:   []string{"KEY"},
			Geometry:      b.geometry(pair.Key.Box),
			Relationships: keyRels,
		})
	}
}

func looksLikeKey(text string) bool {
	text = strings.TrimSpace(text)
	if text == "" {
		return false
	}
	if strings.HasSuffix(text, ":") {
		return true
	}
	return len(strings.Fields(text)) <= 4
}

func sameBand(a, b image.Rectangle) bool {
	aMid := (a.Min.Y + a.Max.Y) / 2
	return aMid >= b.Min.Y && aMid <= b.Max.Y
}

// findValueToRight finds the nearest unclaimed line to the right of key,
// within the same horizontal text band.
func findValueToRight(key placedLine, lines []placedLine, claimed map[string]bool, inTable func(image.Rectangle) bool) *placedLine {
	var best *placedLine
	bestDist := 0
	for i := range lines {
		candidate := lines[i]
		if candidate.ID == key.ID || claimed[candidate.ID] || inTable(candidate.Box) {
			continue
		}
		if candidate.Box.Min.X <= key.Box.Max.X || !sameBand(key.Box, candidate.Box) {
			continue
		}
		dist := candidate.Box.Min.X - key.Box.Max.X
		if best == nil || dist < bestDist {
			c := candidate
			best = &c
			bestDist = dist
		}
	}
	return best
}

// findValueBelow finds the nearest unclaimed line directly below key with
// a similar left edge, used when no value is found to the right.
func findValueBelow(key placedLine, lines []placedLine, claimed map[string]bool, inTable func(image.Rectangle) bool) *placedLine {
	var best *placedLine
	bestDist := 0
	for i := range lines {
		candidate := lines[i]
		if candidate.ID == key.ID || claimed[candidate.ID] || inTable(candidate.Box) {
			continue
		}
		if candidate.Box.Min.Y <= key.Box.Max.Y {
			continue
		}
		if abs(candidate.Box.Min.X-key.Box.Min.X) > 20 {
			continue
		}
		dist := candidate.Box.Min.Y - key.Box.Max.Y
		if dist > key.Box.Dy()*3 {
			continue
		}
		if best == nil || dist < bestDist {
			c := candidate
			best = &c
			bestDist = dist
		}
	}
	return best
}

// addSelectionElements emits SELECTION_ELEMENT blocks for checkbox/radio-
// button-shaped regions outside any detected table.
func (p *Processor) addSelectionElements(b *builder, bin *gocv.Mat, exclude []image.Rectangle) {
	for _, sel := range DetectSelectionElements(bin, exclude) {
		status := "NOT_SELECTED"
		if sel.Selected {
			status = "SELECTED"
		}
		b.add(Block{
			BlockType:       "SELECTION_ELEMENT",
			Confidence:      structuralConfidence,
			SelectionStatus: status,
			Geometry:        b.geometry(sel.Box),
		})
	}
}

func relIfAny(typ string, ids []string) []Relationship {
	if len(ids) == 0 {
		return nil
	}
	return []Relationship{{Type: typ, Ids: ids}}
}

func abs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
