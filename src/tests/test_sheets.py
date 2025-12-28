from src.integrations.google_sheets import get_gspread_client

SPREADSHEET_ID = "1UoQ-uPHOoCsXoHkk6AUdioMTmpQa9m6dZPLJY3EtPRM"

gc = get_gspread_client()
sh = gc.open_by_key(SPREADSHEET_ID)
ws = sh.sheet1

ws.update(
    range_name="A1",
    values=[["OAuth works ✅"]],
)
print("Success")