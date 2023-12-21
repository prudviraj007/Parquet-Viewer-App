import os
import tkinter as tk
from tkinter import filedialog, ttk
import pandas as pd
import pyarrow.parquet as pq
import glob
import tkinter.messagebox
from PIL import Image, ImageTk

def open_folder():
    folder_path = filedialog.askdirectory()
    if folder_path:
        parquet_files = glob.glob(os.path.join(folder_path, '*.parquet'))
        data = pd.concat((pd.read_parquet(file) for file in parquet_files))
        update_tree(data)
        update_info(folder_path, data)

def open_file():
    file_path = filedialog.askopenfilename(filetypes=[("Parquet files", "*.parquet")])
    if file_path:
        data = pd.read_parquet(file_path)
        update_tree(data)
        update_info(os.path.dirname(file_path), data)

def copy_to_clipboard():
    selected_items = tree.selection()
    if not selected_items:
        tkinter.messagebox.showinfo("No Selection", "Please select rows to copy.")
        return

    data = []
    for item in selected_items:
        values = tree.item(item, 'values')
        data.append('\t'.join(map(str, values)))

    clipboard_data = '\n'.join(data)
    root.clipboard_clear()
    root.clipboard_append(clipboard_data)
    root.update()

def update_tree(data):
    tree.delete(*tree.get_children())
    tree["column"] = list(data.columns)
    tree["show"] = "headings"
    for column in tree["columns"]:
        tree.heading(column, text=column)
    df_rows = data.to_numpy().tolist()
    for row in df_rows:
        tree.insert("", "end", values=row)

def update_info(folder_path, data):
    info.delete('1.0', tk.END)
    info.insert(tk.END, f'Folder/File: {os.path.basename(folder_path)}\n')
    info.insert(tk.END, f'Rows: {len(data)}\n')
    info.insert(tk.END, f'Columns: {len(data.columns)}\n')  # Display columns count on the same line
    for column in data.columns:
        info.insert(tk.END, f'  {column}\n')

root = tk.Tk()
root.geometry('800x600')
root.title('Parquet Viewer')

# Load icons using Pillow
folder_icon = ImageTk.PhotoImage(Image.open("folder_icon.png").resize((24, 24)))
file_icon = ImageTk.PhotoImage(Image.open("file_icon.jpeg").resize((24, 24)))
copy_icon = ImageTk.PhotoImage(Image.open("copy_icon.jpeg").resize((24, 24)))

# Frame for top row buttons
top_frame = tk.Frame(root)
top_frame.grid(row=0, column=0, columnspan=2, pady=5)

# Top row buttons
open_folder_button = tk.Button(top_frame, text="Open Folder", image=folder_icon, compound="top", command=open_folder)
open_folder_button.grid(row=0, column=0, padx=5)

open_file_button = tk.Button(top_frame, text="Open File", image=file_icon, compound="top", command=open_file)
open_file_button.grid(row=0, column=1, padx=5)

copy_button = tk.Button(top_frame, text="Copy to Clipboard", image=copy_icon, compound="top", command=copy_to_clipboard)
copy_button.grid(row=0, column=2, padx=5)

# Information frame
info_frame = tk.Frame(root)
info_frame.grid(row=1, column=0, pady=5, padx=5)

info = tk.Text(info_frame, width=30)
info.pack(side='top', fill='y')

# Treeview frame
frame = tk.Frame(root)
frame.grid(row=1, column=1, pady=5, padx=5, sticky='nsew')

tree = ttk.Treeview(frame)
tree.pack(side='left', fill='both', expand=True)

vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
vsb.pack(side='right', fill='y')

hsb = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
hsb.pack(side='bottom', fill='x')

tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

# Make the frame and tree expandable
root.grid_rowconfigure(1, weight=1)
root.grid_columnconfigure(1, weight=1)

root.mainloop()
