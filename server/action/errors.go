package action

import "fmt"

type IndexNotFoundError struct {
	Index string
}

func (e *IndexNotFoundError) Error() string {
	return fmt.Sprintf("no such index [%s]", e.Index)
}

type IndexAlreadyExistsError struct {
	Index string
}

func (e *IndexAlreadyExistsError) Error() string {
	return fmt.Sprintf("index [%s] already exists", e.Index)
}

type InvalidIndexNameError struct {
	Index  string
	Reason string
}

func (e *InvalidIndexNameError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("invalid index name [%s]: %s", e.Index, e.Reason)
	}
	return fmt.Sprintf("invalid index name [%s]", e.Index)
}

type QueryParsingError struct {
	Reason string
}

func (e *QueryParsingError) Error() string {
	return fmt.Sprintf("parse query: %s", e.Reason)
}

type MapperParsingError struct {
	Reason string
}

func (e *MapperParsingError) Error() string {
	return e.Reason
}
