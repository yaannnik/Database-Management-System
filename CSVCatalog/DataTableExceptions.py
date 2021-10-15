class DataTableException(Exception):

    invalid_column_definition   =   -100
    duplicate_table_name        =   -101
    not_implemented             =   -200
    invalid_file                =   -300

    def __init__(self, code=None, message=None, ex=None):
        self.code = code
        self.message = message
        self.original_exception = ex

    def __str__(self):
        """
        TODO We should map MySQL and infrastructure exceptions to more meaningful exceptions.
        :return:
        """
        result = ""

        if self.code:
            self.code = str(self.code)
        else:
            self.code = "None"

        if self.message is None:
            self.messsage = "None"

        result += "DataTableException: code: {:<5}, message: {}".format(self.code, self.message)

        if self.original_exception is not None:
            result += "\nOriginal exception = " + repr(self.original_exception)

        return result