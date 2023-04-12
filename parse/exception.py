class OpenAnIssueIfYouGetThisError(Exception):

    def __init__(self, message: str):
        docstr = self.__doc__ or ""
        self.message = message + "\n\n" + docstr
        super().__init__(message)

    pass


class JsonQLException(Exception):
    pass


class JsonQLRuntimeError(JsonQLException):
    pass


class JsonQLReferenceError(JsonQLException):
    pass


class JsonQLTypeError(JsonQLRuntimeError):
    pass