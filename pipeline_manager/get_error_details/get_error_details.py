

def get_error_details(exception: Exception) -> str:
    """
    Gets exception object and finds out error type, error file location, error line and message. This method is used
    to capture original error location and returns it in str format. This way we would be able to see where exactly
    error happened without crashing the program
    :param exception: Exception object
    :return:
    """
    error_type = type(exception).__name__
    error_file_location = exception.__traceback__.tb_frame.f_code.co_filename
    error_file_location_formatted = error_file_location.split("\\")[-1]
    error_line = exception.__traceback__.tb_lineno
    error_message = f"{str(exception)}"
    error_text: str = f"Error Type: {error_type}, " \
                      f"Error File Location: {error_file_location_formatted}, " \
                      f"Error Line: {error_line}, " \
                      f"Error Message: {truncate_error_message(error_message)}"
    return error_text


def truncate_error_message(error_message: str) -> str:
    """
    Error message truncated for avoiding the overflowing the error file
    :param error_message: string
    :return:
    """
    return error_message[0:500]
