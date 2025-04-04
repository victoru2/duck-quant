# Google Sheets to DuckDB Pipeline

## 🔐 Google Sheets API Credentials Setup

### **1. Create a Google Cloud Project**
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create new project or select existing one

### **2. Enable Google Sheets API**
```bash
Navigation Path: APIs & Services > Library > Google Sheets API > Enable
```

### **3. Create Service Account**
```bash
Navigation Path: APIs & Services > Credentials > + CREATE CREDENTIALS > Service Account
```

### **4. Generate JSON Key**
1. In service account list, click Actions > Manage Keys
2. Select ADD KEY > Create new key
3. Choose JSON format → Download

### **5. Share Spreadsheet**
1. Open target Google Sheet
2. Click Share → Add service account email (from JSON's client_email)
3. Set permission level: Reader

### **6. Run Extraction**
```bash
export DUCKDB_PATH="../data/database.duckdb"
python gsheets.py '{"id":"my-id", "range":"range!A:O"}'
```
