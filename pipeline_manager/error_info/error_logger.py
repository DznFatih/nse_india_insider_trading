

def get_original_error_message(exception: Exception) -> str:
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
    return error_message[0:500]
