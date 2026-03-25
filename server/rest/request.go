// server/rest/request.go
package rest

type RestRequest struct {
	Method string
	Params map[string]string
	Body   []byte
}
