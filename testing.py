import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pprint import pprint

scope = [
    "https://spreadsheets.google.com/feeds",
    'https://www.googleapis.com/auth/spreadsheets',
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"
    ]

credentials = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)

client = gspread.authorize(credentials)
sheet = client.open("tttesting").sheet1
data = sheet.get_all_records()


for x in range(1,7):
    initalize_sheet = ['fuck' + str(x), 'cuz why not' + str(x), 'parrot' + str(x), 'dog' + str(x), 12 + x]
    sheet.insert_row(initalize_sheet, x)


# # print full row (second testingrow)
# row = sheet.row_values(2)
# pprint(row)

# # print full col (first col)
# col = sheet.col_values(1)
# pprint(col)

# # print contents of a cell (format (row, column))
# cell = sheet.cell(5,2).value
# pprint(cell)

# # or
# cell2 = sheet.get('A2:C2').value
# print(cell2)

# # Insert Row
# instertRow = ["Hello", 5, 'red', 'World']
# sheet.insert_row(instertRow, 4)

# # delete Row (deleters row 4)
# sheet.delete_rows(4, 5)

# # update cell
# sheet.update_cell(3, 2, 'CHANGED')

# # Find how many rows have data
# pprint(len(data))

# # print full data
# pprint(data)
