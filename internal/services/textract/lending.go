// Lending analysis emulates Textract's loan-document classification/extraction
// as a one-page "package," classified via keyword search instead of an ML model.
package textract

import "strings"

// lendingKeywords maps a keyword found in a page's OCR text to Textract's
// documented LendingDocumentType. Checked in order; first match wins.
var lendingKeywords = []struct {
	keyword string
	docType string
}{
	{"closing disclosure", "CLOSING_DISCLOSURE"},
	{"loan estimate", "LOAN_ESTIMATE"},
	{"promissory note", "PROMISSORY_NOTE"},
	{"deed of trust", "DEED_OF_TRUST"},
	{"mortgage note", "PROMISSORY_NOTE"},
	{"1003", "URLA_1003"},
	{"uniform residential loan application", "URLA_1003"},
	{"w-2", "W2"},
	{"w2", "W2"},
	{"paystub", "PAYSTUB"},
	{"pay stub", "PAYSTUB"},
	{"bank statement", "BANK_STATEMENT"},
	{"appraisal", "APPRAISAL"},
	{"flood determination", "FLOOD_DETERMINATION"},
	{"homeowners insurance", "HOMEOWNERS_INSURANCE"},
}

// classifyDocument returns the first LendingDocumentType whose keyword
// appears in any OCR'd line, or "UNCLASSIFIED_TYPE" if none match.
func classifyDocument(lines []placedLine) string {
	var all strings.Builder
	for _, l := range lines {
		all.WriteString(strings.ToLower(l.Text))
		all.WriteByte('\n')
	}
	text := all.String()
	for _, k := range lendingKeywords {
		if strings.Contains(text, k.keyword) {
			return k.docType
		}
	}
	return "UNCLASSIFIED_TYPE"
}

type PredictedElement struct {
	Value      string  `json:"Value"`
	Confidence float64 `json:"Confidence"`
}

type PageClassification struct {
	PageType   []PredictedElement `json:"PageType"`
	PageNumber []PredictedElement `json:"PageNumber"`
}

type LendingDetection struct {
	Text            string   `json:"Text"`
	SelectionStatus string   `json:"SelectionStatus,omitempty"`
	Geometry        Geometry `json:"Geometry"`
	Confidence      float64  `json:"Confidence"`
}

type LendingField struct {
	Type            string             `json:"Type"`
	KeyDetection    *LendingDetection  `json:"KeyDetection,omitempty"`
	ValueDetections []LendingDetection `json:"ValueDetections,omitempty"`
}

type LendingDocument struct {
	LendingFields []LendingField `json:"LendingFields"`
}

type Extraction struct {
	LendingDocument *LendingDocument `json:"LendingDocument,omitempty"`
}

type LendingResult struct {
	Page               int                `json:"Page"`
	PageClassification PageClassification `json:"PageClassification"`
	Extractions        []Extraction       `json:"Extractions"`
}

// SplitDocument identifies which pages of the input package make up one
// logical document within a DocumentGroup.
type SplitDocument struct {
	Index int   `json:"Index"`
	Pages []int `json:"Pages"`
}

// DocumentGroup mirrors Textract's GetLendingAnalysisSummary grouping of
// pages classified as the same DocumentType.
type DocumentGroup struct {
	Type           string          `json:"Type"`
	SplitDocuments []SplitDocument `json:"SplitDocuments"`
}

// LendingSummary mirrors GetLendingAnalysisSummary's top-level Summary shape.
// UndetectedDocumentTypes is always empty: classifyDocument never returns "undetected".
type LendingSummary struct {
	DocumentGroups          []DocumentGroup `json:"DocumentGroups"`
	UndetectedDocumentTypes []string        `json:"UndetectedDocumentTypes"`
}

// analyzeLending runs the shared OCR + key/value pairing pipeline, then
// classifies the page and reshapes matched fields into LendingFields.
func (p *Processor) analyzeLending(raw []byte) (*LendingResult, error) {
	_, lines, _, bin, err := p.buildTextBlocks(raw)
	if err != nil {
		return nil, err
	}
	tableRects := DetectTableGrids(bin)
	underlines := DetectFieldUnderlines(bin, tableRects)
	defer bin.Close()

	docType := classifyDocument(lines)

	var fields []LendingField
	for _, pair := range pairKeyValues(lines, tableRects, underlines) {
		labelText := strings.TrimSuffix(strings.TrimSpace(pair.Key.Text), ":")
		fields = append(fields, LendingField{
			Type: expenseTypeFromLabel(labelText),
			KeyDetection: &LendingDetection{
				Text:       labelText,
				Confidence: pair.Key.Confidence,
			},
			ValueDetections: []LendingDetection{{
				Text:       pair.Value.Text,
				Confidence: pair.Value.Confidence,
			}},
		})
	}

	return &LendingResult{
		Page: 1,
		PageClassification: PageClassification{
			PageType:   []PredictedElement{{Value: docType, Confidence: structuralConfidence}},
			PageNumber: []PredictedElement{{Value: "1", Confidence: structuralConfidence}},
		},
		Extractions: []Extraction{{
			LendingDocument: &LendingDocument{LendingFields: fields},
		}},
	}, nil
}
