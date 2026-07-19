// AnalyzeExpense emulates Textract's receipt/invoice analysis by reusing
// the same key/value layout heuristic as the FORMS feature and reshaping
// each match into Textract's ExpenseDocument/SummaryFields shape, rather
// than the generic KEY_VALUE_SET Block tree. Like FORMS/TABLES, field
// *typing* (mapping a label like "Total" to the normalized type
// "TOTAL") is a simple text transform, not the ML classifier real
// Textract uses.
package textract

import (
	"strings"
)

type ExpenseType struct {
	Text       string  `json:"Text"`
	Confidence float64 `json:"Confidence,omitempty"`
}

type ExpenseDetection struct {
	Text       string   `json:"Text"`
	Geometry   Geometry `json:"Geometry"`
	Confidence float64  `json:"Confidence"`
}

type ExpenseField struct {
	Type           ExpenseType       `json:"Type"`
	LabelDetection *ExpenseDetection `json:"LabelDetection,omitempty"`
	ValueDetection ExpenseDetection  `json:"ValueDetection"`
	PageNumber     int               `json:"PageNumber"`
}

type ExpenseDocument struct {
	ExpenseIndex  int            `json:"ExpenseIndex"`
	SummaryFields []ExpenseField `json:"SummaryFields"`
}

type AnalyzeExpenseOutput struct {
	ExpenseDocuments []ExpenseDocument `json:"ExpenseDocuments"`
}

// AnalyzeExpense runs the shared OCR + table-detection pipeline, then pairs
// lines into SummaryFields using the same heuristic as FORMS's
// KEY_VALUE_SET pairing.
func (p *Processor) AnalyzeExpense(raw []byte) (*AnalyzeExpenseOutput, error) {
	b, lines, _, bin, err := p.buildTextBlocks(raw)
	if err != nil {
		return nil, err
	}
	defer bin.Close()

	tableRects := DetectTableGrids(bin)
	underlines := DetectFieldUnderlines(bin, tableRects)

	var fields []ExpenseField
	for _, pair := range pairKeyValues(lines, tableRects, underlines) {
		labelText := strings.TrimSuffix(strings.TrimSpace(pair.Key.Text), ":")
		fields = append(fields, ExpenseField{
			Type: ExpenseType{Text: expenseTypeFromLabel(labelText), Confidence: structuralConfidence},
			LabelDetection: &ExpenseDetection{
				Text:       labelText,
				Geometry:   b.geometry(pair.Key.Box),
				Confidence: pair.Key.Confidence,
			},
			ValueDetection: ExpenseDetection{
				Text:       pair.Value.Text,
				Geometry:   b.geometry(pair.Value.Box),
				Confidence: pair.Value.Confidence,
			},
			PageNumber: 1,
		})
	}

	return &AnalyzeExpenseOutput{
		ExpenseDocuments: []ExpenseDocument{{
			ExpenseIndex:  1,
			SummaryFields: fields,
		}},
	}, nil
}

// expenseTypeFromLabel normalizes a detected label ("Total", "Invoice #")
// into Textract's SCREAMING_SNAKE_CASE Type.Text convention. Real Textract
// maps labels to a fixed vocabulary (TOTAL, VENDOR_NAME, ...) via ML; this
// is a text-shape approximation, not that vocabulary.
func expenseTypeFromLabel(label string) string {
	if label == "" {
		return "OTHER"
	}
	return strings.ToUpper(strings.Join(strings.Fields(label), "_"))
}
