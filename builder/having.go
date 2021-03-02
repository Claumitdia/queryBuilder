package querybuilder

// HavingStruct - struct for having clause
type HavingStruct struct {
	HavingKeyword     string //having
	HavingAndPhrase   []AndStruct
	HavingOrPhrase    []OrStruct
	FinalHavingPhrase string //mix of all above
}
