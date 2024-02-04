
Python version - >=3.12

HOW TO RUN CODE?

1- Go to the main.py file
2- Define from_date and to_date for NSEIndiaInsiderTradingExtractParameter object or don't pass any argument. If no
   argument is passed then it will default from_date = today - 365 and to_date = today
3- Run the code
4- It will generate a folder called XBRL Files. This will be generated automatically. Inside this folder, there will be
   new folder created with current date and time. Inside this folder, there will be XBRL Files with NSEData.txt file
   that has all processed data. This data will be delimited by a vertical bar '|'.
