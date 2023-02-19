import logging
from src.utils import FileOperations


logging.basicConfig(level=logging.INFO, format='%(asctime)s : %(levelname)s : %(name)s : %(message)s')


__author__ = "Carlos Manuel Molina Sotoca"
__email__ = "cmmolinas01@gmail.com"


class Logger:
    def __init__(self,
                 module: str = __name__,
                 level: str = "INFO",
                 logs_file_path: str = None):
        self.logger = logging.getLogger(module)
        self._set_logger_level(level)

        if logs_file_path is not None:
            self.file_path = f'{logs_file_path}/{module}_logfile.log'
            self._set_logs_file()
        else:
            self.file_path = logs_file_path

    def _set_logger_level(self, level):
        if level == "DEBUG":
            self.logger.setLevel(logging.DEBUG)
        elif level == "INFO":
            self.logger.setLevel(logging.INFO)
        elif level == "WARNING":
            self.logger.setLevel(logging.WARNING)
        elif level == "ERROR":
            self.logger.setLevel(logging.ERROR)
        elif level == "CRITICAL":
            self.logger.setLevel(logging.CRITICAL)
        else:
            raise Exception("Logging level does not exist. Supported levels: DEBUG, INFO, WARNING, ERROR and CRITICAL")

    def _set_logs_file(self):
        FileOperations.write_file(self.file_path, "FILE CREATION\n\n")
        if self.file_path is not None:
            # Define file handler and set formatter:
            file_handler = logging.FileHandler(self.file_path)
            formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
            file_handler.setFormatter(formatter)

            # Add file handler to logger:
            self.logger.addHandler(file_handler)

    def set_message(self, level, message_level, message):
        # Modify message depending on level:
        if message_level == "SECTION":
            message = f"\n ########## {message.upper()} ##########\n"
        elif message_level == "SUBSECTION":
            message = f"\n ---------- {message} ----------\n"
        else:
            message = f"\n {message}\n"

        # Write message:
        if level == "DEBUG":
            self.logger.debug(message)
        elif level == "INFO":
            self.logger.info(message)
        elif level == "WARNING":
            self.logger.warning(message)
        elif level == "ERROR":
            self.logger.error(message)
        elif level == "CRITICAL":
            self.logger.critical(message)
        else:
            raise Exception("Logging level does not exist. Supported levels: DEBUG, INFO, WARNING, ERROR and CRITICAL")
