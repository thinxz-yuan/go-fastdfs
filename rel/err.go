package rel

type Err struct {
	error string
}

func NewErr(error string) *Err {
	return &Err{
		error: error,
	}
}

func (e Err) Error() string {
	return e.error
}
