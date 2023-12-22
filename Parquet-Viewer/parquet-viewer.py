import os
import tkinter as tk
from tkinter import filedialog, ttk
import dask.dataframe as dd
import pandas as pd
import tkinter.messagebox
from PIL import Image, ImageTk
from tqdm import tqdm
from threading import Thread

root = tk.Tk()
root.geometry('800x600')
root.title('Parquet Viewer')

# Load icons using Pillow
folder_icon = ImageTk.PhotoImage(Image.open("folder_icon.png").resize((24, 24)))
file_icon = ImageTk.PhotoImage(Image.open("file_icon.jpeg").resize((24, 24)))
copy_icon = ImageTk.PhotoImage(Image.open("copy_icon.jpeg").resize((24, 24)))
select_all_icon = ImageTk.PhotoImage(Image.open("selectall_icon.png").resize((24, 24)))

def open_folder():
    folder_path = filedialog.askdirectory()
    if folder_path:
        folder_files = os.listdir(folder_path)
        parquet_files = [os.path.join(folder_path, file) for file in folder_files if file.endswith('.parquet') and os.path.isfile(os.path.join(folder_path, file))]

        if not parquet_files:
            tkinter.messagebox.showinfo("No Parquet Files", "No Parquet files found in the selected folder.")
            return

        try:
            data_frames = []

            def load_files():
                for file in tqdm(parquet_files, desc="Loading Files", unit="file"):
                    try:
                        dask_data = dd.read_parquet(file, engine='pyarrow')
                        df = dask_data.compute()
                        data_frames.append(df)
                    except Exception as e:
                        tkinter.messagebox.showwarning("Error Reading File", f"Error reading file '{file}': {str(e)}")

                progress_bar.stop()
                progress_bar.destroy()

                data = pd.concat(data_frames)
                update_tree(data)
                update_info(folder_path, data)

            progress_bar = ttk.Progressbar(root, mode='indeterminate')
            progress_bar.grid(row=2, column=0, columnspan=4, pady=5, sticky='ew')
            progress_bar.start()

            # Run loading files in a separate thread
            Thread(target=load_files).start()

        except Exception as e:
            tkinter.messagebox.showwarning("Error Reading Files", f"Error reading files: {str(e)}")

def open_file():
    file_path = filedialog.askopenfilename(filetypes=[("Parquet files", "*.parquet")])
    if file_path:
        try:
            data = pd.read_parquet(file_path)
            update_tree(data)
            update_info(os.path.dirname(file_path), data)
        except Exception as e:
            tkinter.messagebox.showwarning("Error Reading File", f"Error reading file '{file_path}': {str(e)}")

def select_all_rows():
    items = tree.get_children()
    tree.selection_set(items)

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

    # Display the first 1000 rows
    df_rows = data.head(1000).to_numpy().tolist()
    
    for row in df_rows:
        tree.insert("", "end", values=row)

def update_info(folder_path, data):
    info.delete('1.0', tk.END)
    info.insert(tk.END, f'Folder/File: {os.path.basename(folder_path)}\n')
    info.insert(tk.END, f'Rows: {len(data)}\n')
    info.insert(tk.END, f'Columns: {len(data.columns)}\n')  # Display columns count on the same line
    for column in data.columns:
        info.insert(tk.END, f'  {column}\n')

# Frame for top row buttons
top_button_frame = ttk.Frame(root)
top_button_frame.grid(row=0, column=0, columnspan=4, pady=5)

# Top row buttons
open_folder_button = tk.Button(top_button_frame, text="Open Folder", image=folder_icon, compound="top", command=open_folder)
open_folder_button.grid(row=0, column=0, padx=5)

open_file_button = tk.Button(top_button_frame, text="Open File", image=file_icon, compound="top", command=open_file)
open_file_button.grid(row=0, column=1, padx=5)

select_all_button = tk.Button(top_button_frame, text="Select All", image=select_all_icon, compound="top", command=select_all_rows)
select_all_button.grid(row=0, column=2, padx=5)

copy_button = tk.Button(top_button_frame, text="Copy to Clipboard", image=copy_icon, compound="top", command=copy_to_clipboard)
copy_button.grid(row=0, column=3, padx=5)

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
