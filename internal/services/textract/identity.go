// AnalyzeID emulates Textract's identity-document analysis by reusing the
// FORMS/AnalyzeExpense key/value heuristic, reshaped into IdentityDocumentField.
package textract

import (
	"strings"
)

type IdentityDocumentFieldType struct {
	Text string `json:"Text"`
}

type IdentityDocumentValueDetection struct {
	Text       string   `json:"Text"`
	Geometry   Geometry `json:"Geometry,omitempty"`
	Confidence float64  `json:"Confidence"`
}

type IdentityDocumentField struct {
	Type           IdentityDocumentFieldType      `json:"Type"`
	ValueDetection IdentityDocumentValueDetection `json:"ValueDetection"`
}

type IdentityDocument struct {
	DocumentIndex          int                     `json:"DocumentIndex"`
	IdentityDocumentFields []IdentityDocumentField `json:"IdentityDocumentFields"`
	Blocks                 []Block                 `json:"Blocks,omitempty"`
}

type AnalyzeIDOutput struct {
	IdentityDocuments     []IdentityDocument `json:"IdentityDocuments"`
	AnalyzeIDModelVersion string             `json:"AnalyzeIDModelVersion"`
}

// AnalyzeID runs the shared OCR + key/value pairing pipeline over each of
// the supplied document pages, one IdentityDocument per page.
func (p *Processor) AnalyzeID(pages [][]byte) (*AnalyzeIDOutput, error) {
	docs := make([]IdentityDocument, 0, len(pages))
	for i, raw := range pages {
		b, lines, _, bin, err := p.buildTextBlocks(raw)
		if err != nil {
			return nil, err
		}
		tableRects := DetectTableGrids(bin)
		underlines := DetectFieldUnderlines(bin, tableRects)
		bin.Close()

		var fields []IdentityDocumentField
		for _, pair := range pairKeyValues(lines, tableRects, underlines) {
			labelText := strings.TrimSuffix(strings.TrimSpace(pair.Key.Text), ":")
			fields = append(fields, IdentityDocumentField{
				Type: IdentityDocumentFieldType{Text: identityTypeFromLabel(labelText)},
				ValueDetection: IdentityDocumentValueDetection{
					Text:       pair.Value.Text,
					Geometry:   b.geometry(pair.Value.Box),
					Confidence: pair.Value.Confidence,
				},
			})
		}

		docs = append(docs, IdentityDocument{
			DocumentIndex:          i + 1,
			IdentityDocumentFields: fields,
			Blocks:                 b.blocks,
		})
	}

	return &AnalyzeIDOutput{
		IdentityDocuments:     docs,
		AnalyzeIDModelVersion: modelVersion,
	}, nil
}

// identityLabelTypes maps common ID-document label text to Textract's
// documented AnalyzeID field types; unmatched labels fall back to SCREAMING_SNAKE_CASE.
var identityLabelTypes = map[string]string{
	"first name":      "FIRST_NAME",
	"given name":      "FIRST_NAME",
	"last name":       "LAST_NAME",
	"surname":         "LAST_NAME",
	"middle name":     "MIDDLE_NAME",
	"date of birth":   "DATE_OF_BIRTH",
	"dob":             "DATE_OF_BIRTH",
	"date of issue":   "DATE_OF_ISSUE",
	"date of expiry":  "EXPIRATION_DATE",
	"expiration date": "EXPIRATION_DATE",
	"exp":             "EXPIRATION_DATE",
	"document number": "DOCUMENT_NUMBER",
	"id number":       "DOCUMENT_NUMBER",
	"license number":  "DOCUMENT_NUMBER",
	"address":         "ADDRESS",
	"county":          "COUNTY",
	"place of birth":  "PLACE_OF_BIRTH",
	"state name":      "STATE_NAME",
	"state in id":     "STATE_IN_ID",
	"class":           "CLASS",
	"sex":             "SEX",
	"height":          "HEIGHT",
	"id type":         "ID_TYPE",
}

func identityTypeFromLabel(label string) string {
	if label == "" {
		return "OTHER"
	}
	if t, ok := identityLabelTypes[strings.ToLower(label)]; ok {
		return t
	}
	return strings.ToUpper(strings.Join(strings.Fields(label), "_"))
}
