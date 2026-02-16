import customtkinter as ctk
from tkinter import filedialog, messagebox
import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime
import traceback

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

class AdvancedDataMigrationTool(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("DataMigration Pro: CSV to SQL Ecosystem")
        self.geometry("600x600")
        
        self.log_folder = "system_logs"
        os.makedirs(self.log_folder, exist_ok=True)
        self.log_event("INITIALIZATION: Migration Engine Started.")

        # --- UI ELEMENTS ---
        self.label = ctk.CTkLabel(self, text="DATA MIGRATION ENGINE", font=("Arial", 22, "bold"))
        self.label.pack(pady=20)

        self.select_btn = ctk.CTkButton(self, text="1. Load Source CSV", command=self.select_file)
        self.select_btn.pack(pady=10)

        self.file_label = ctk.CTkLabel(self, text="No source file detected", text_color="gray")
        self.file_label.pack(pady=5)

        self.option_label = ctk.CTkLabel(self, text="2. Select Output Format:", font=("Arial", 12, "bold"))
        self.option_label.pack(pady=(15, 5))

        self.format_var = ctk.StringVar(value="SQLite Database (.db)")
        self.format_menu = ctk.CTkOptionMenu(self, 
                                            values=["SQLite Database (.db)", "Standard SQL Script (.sql)"],
                                            variable=self.format_var)
        self.format_menu.pack(pady=10)

        self.refresh_btn = ctk.CTkButton(self, text="Clear Session", command=self.reset_app, 
                                         fg_color="#444", hover_color="#666")
        self.refresh_btn.pack(pady=10)

        self.convert_btn = ctk.CTkButton(self, text="3. Execute Schema Migration", 
                                          command=self.process_data, state="disabled",
                                          fg_color="#27ae60", hover_color="#2ecc71")
        self.convert_btn.pack(pady=25)

        self.status_label = ctk.CTkLabel(self, text="System Status: Ready", text_color="cyan")
        self.status_label.pack(side="bottom", pady=20)

        self.input_path = ""

    def log_event(self, message, is_error=False):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        date_str = datetime.now().strftime("%Y-%m-%d")
        log_file = os.path.join(self.log_folder, f"migration_log_{date_str}.txt")
        prefix = "[ERROR]" if is_error else "[INFO]"
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"{timestamp} {prefix} {message}\n")

    def reset_app(self):
        self.input_path = ""
        self.file_label.configure(text="No source file detected", text_color="gray")
        self.convert_btn.configure(state="disabled")
        self.status_label.configure(text="System: Reset Complete", text_color="cyan")
        messagebox.showinfo("Reset", "Session data cleared.")

    def select_file(self):
        path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if path:
            self.input_path = path
            self.file_label.configure(text=os.path.basename(path), text_color="white")
            self.convert_btn.configure(state="normal")
            self.log_event(f"SOURCE_LOADED: {os.path.basename(path)}")

    def smart_read_csv(self, path):
        """Standardized encoding detection for robust data ingestion."""
        encodings = ['utf-8-sig', 'utf-8', 'latin1', 'cp1252']
        for enc in encodings:
            try:
                df = pd.read_csv(path, encoding=enc)
                self.log_event(f"DECODER: Successful ingestion using '{enc}' encoding.")
                return df
            except UnicodeDecodeError:
                continue
        raise Exception("Source encoding not recognized. Please check file format.")

    def generate_sql_script(self, df_dict):
        sql_output = "-- DATA MIGRATION ENGINE GENERATED SCRIPT --\n"
        sql_output += "SET NAMES utf8mb4;\n\n"
        
        for table_name, data in df_dict.items():
            schema = pd.io.sql.get_schema(data, table_name).replace('"', '`')
            sql_output += f"DROP TABLE IF EXISTS `{table_name}`;\n{schema};\n\n"
            
            for _, row in data.iterrows():
                cols = '`, `'.join(row.index)
                cleaned_vals = []
                for val in row.values:
                    if pd.isna(val): cleaned_vals.append("NULL")
                    elif isinstance(val, (int, float)): cleaned_vals.append(str(val))
                    else:
                        v = str(val).replace("'", "''")
                        cleaned_vals.append(f"'{v}'")
                
                vals_str = ", ".join(cleaned_vals)
                sql_output += f"INSERT INTO `{table_name}` (`{cols}`) VALUES ({vals_str});\n"
            sql_output += "\n"
        return sql_output

    def process_data(self):
        try:
            chosen_format = self.format_var.get()
            is_sql_script = ".sql" in chosen_format
            ext = ".sql" if is_sql_script else ".db"
            
            output_path = filedialog.asksaveasfilename(defaultextension=ext, filetypes=[("Export File", f"*{ext}")])
            if not output_path: return

            self.status_label.configure(text="Status: Analyzing Data Schema...")
            self.update()

            df = self.smart_read_csv(self.input_path)
            
            # Date Normalization
            for col in df.columns:
                if 'date' in col.lower() or 'time' in col.lower():
                    df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

            # Dynamic Schema Mapping
            groups = {
                "Client_Entities": ["customer", "user", "client", "buyer"],
                "Inventory_Assets": ["product", "item", "sku", "category", "sub-category"],
                "Geographic_Data": ["city", "state", "country", "postal", "region", "address"]
            }

            tables_to_save = {}
            key_columns = [] 

            for table_name, keywords in groups.items():
                matched_cols = [c for c in df.columns if any(k in c.lower() for k in keywords)]
                if matched_cols:
                    tables_to_save[table_name] = df[matched_cols].drop_duplicates()
                    id_cols = [c for c in matched_cols if 'id' in c.lower() or 'name' in c.lower() or 'sku' in c.lower()]
                    key_columns.extend(id_cols)

            # Relational Mapping for Transactional Data
            transactional_keywords = ["order", "ship", "transaction", "date", "sales", "profit", "unit", "price"]
            trans_cols = [c for c in df.columns if any(k in c.lower() for k in transactional_keywords)]
            
            final_trans_cols = list(set(trans_cols + key_columns))
            tables_to_save["Transactions"] = df[final_trans_cols]
            
            tables_to_save['Raw_Staging_Data'] = df

            # Export Processing
            if not is_sql_script:
                engine = create_engine(f'sqlite:///{output_path}')
                for name, data in tables_to_save.items():
                    data.to_sql(name, engine, index=False, if_exists='replace')
            else:
                script = self.generate_sql_script(tables_to_save)
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(script)

            self.status_label.configure(text="Status: Migration Successful", text_color="green")
            self.log_event(f"MIGRATION_SUCCESS: {output_path}")
            messagebox.showinfo("Success", "Schema migration complete.\nRelational integrity maintained.")

        except Exception as e:
            self.log_event(traceback.format_exc(), is_error=True)
            messagebox.showerror("Migration Error", f"Fatal Exception: {str(e)}")
            self.status_label.configure(text="Status: System Failure", text_color="red")

if __name__ == "__main__":
    app = AdvancedDataMigrationTool()
    app.mainloop()
