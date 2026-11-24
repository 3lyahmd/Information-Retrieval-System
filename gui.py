
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import csv
import os
import sys
import subprocess
from pathlib import Path

from logic.functions import get_paths


# ==========================================
# 1. PROJECT PATHS
# ==========================================

base_dir, dataset_path, output_path = get_paths()

BASE_DIR = Path(base_dir)
OUTPUT_DIR = Path(output_path)
DATASET_DIR = Path(dataset_path)


def prepare_dataset_and_cache():
    """Choose a dataset folder, then let main.py validate/rebuild its cache."""

    picker = tk.Tk()
    picker.withdraw()
    picker.attributes("-topmost", True)

    selected_dataset = filedialog.askdirectory(
        title="Select Dataset Folder (Cancel = use default folder)",
        initialdir=str(DATASET_DIR)
    )

    picker.destroy()

    # Cancel means use the project's default 'data set' directory.
    if not selected_dataset:
        selected_dataset = str(DATASET_DIR)

    selected_dataset = os.path.abspath(selected_dataset)

    if not os.path.isdir(selected_dataset):
        error_root = tk.Tk()
        error_root.withdraw()
        messagebox.showerror(
            "Dataset Error",
            f"Dataset folder does not exist:\n{selected_dataset}"
        )
        error_root.destroy()
        raise SystemExit(1)

    text_files = [
        name for name in os.listdir(selected_dataset)
        if name.lower().endswith(".txt")
        and os.path.isfile(os.path.join(selected_dataset, name))
    ]

    if not text_files:
        error_root = tk.Tk()
        error_root.withdraw()
        messagebox.showerror(
            "Dataset Error",
            "The selected folder does not contain any .txt documents."
        )
        error_root.destroy()
        raise SystemExit(1)

    # main.py asks for the dataset path in Terminal. Feed the selected path
    # to that prompt so the same cache/signature logic remains authoritative.
    main_file = BASE_DIR / "main.py"

    if not main_file.is_file():
        error_root = tk.Tk()
        error_root.withdraw()
        messagebox.showerror(
            "Startup Error",
            f"main.py was not found:\n{main_file}"
        )
        error_root.destroy()
        raise SystemExit(1)

    print("\n========== GUI DATASET STARTUP ==========\n")
    print("Selected Dataset:", selected_dataset)
    print("Checking cache / generating missing results...\n")

    try:
        result = subprocess.run(
            [sys.executable, "-u", str(main_file)],
            cwd=str(BASE_DIR),
            input=selected_dataset + "\n",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace"
        )

        print(result.stdout)

        if result.returncode != 0:
            error_root = tk.Tk()
            error_root.withdraw()
            messagebox.showerror(
                "Project Startup Failed",
                "main.py could not prepare the project data.\n\n"
                "Check the Terminal output for details.\n\n"
                + result.stdout[-3000:]
            )
            error_root.destroy()
            raise SystemExit(result.returncode)

    except OSError as error:
        error_root = tk.Tk()
        error_root.withdraw()
        messagebox.showerror("Startup Error", str(error))
        error_root.destroy()
        raise SystemExit(1)

    return Path(selected_dataset)


# Run the main pipeline before loading the GUI.
# If the cache is valid, main.py loads saved CSVs and skips Spark.
DATASET_DIR = prepare_dataset_and_cache()


# ==========================================
# 2. COLORS
# ==========================================

# Dark navy + cyan theme inspired by the reference dashboard
SIDEBAR_COLOR = "#0B1428"
BG_COLOR = "#0A1020"
TEXT_COLOR = "#EAF2FF"
ACCENT_COLOR = "#00E5F0"
WHITE = "#14213A"          # Reused as the main panel/card color
MUTED_COLOR = "#91A4C5"
BORDER_COLOR = "#263653"
HOVER_COLOR = "#1B3152"


# ==========================================
# 3. MAIN WINDOW
# ==========================================

root = tk.Tk()

root.title("Information Retrieval System")
root.geometry("1400x850")
root.minsize(1100, 700)
root.configure(bg=BG_COLOR)


# ==========================================
# 4. STYLE
# ==========================================

style = ttk.Style()

style.theme_use("clam")

style.configure(
    "Treeview",
    background=WHITE,
    foreground=TEXT_COLOR,
    rowheight=34,
    fieldbackground=WHITE,
    bordercolor=BORDER_COLOR,
    lightcolor=BORDER_COLOR,
    darkcolor=BORDER_COLOR,
    font=("Arial", 10)
)

style.configure(
    "Treeview.Heading",
    background="#1B2B49",
    foreground=ACCENT_COLOR,
    font=("Arial", 10, "bold"),
    padding=10,
    relief="flat"
)

style.map(
    "Treeview",
    background=[("selected", "#155E75")],
    foreground=[("selected", "#FFFFFF")]
)

style.configure(
    "TEntry",
    fieldbackground="#0E1A30",
    foreground=TEXT_COLOR,
    insertcolor=ACCENT_COLOR,
    bordercolor=BORDER_COLOR,
    lightcolor=BORDER_COLOR,
    darkcolor=BORDER_COLOR,
    padding=8
)

style.configure(
    "TScrollbar",
    background="#1B2B49",
    troughcolor=BG_COLOR,
    bordercolor=BG_COLOR,
    arrowcolor=ACCENT_COLOR
)


# ==========================================
# 5. SIDEBAR
# ==========================================

sidebar = tk.Frame(
    root,
    bg=SIDEBAR_COLOR,
    width=235
)

sidebar.pack(side="left", fill="y")
sidebar.pack_propagate(False)


logo = tk.Label(
    sidebar,
    text="IR SYSTEM",
    font=("Arial", 21, "bold"),
    bg=SIDEBAR_COLOR,
    fg=WHITE
)

logo.pack(pady=(30, 5))


subtitle = tk.Label(
    sidebar,
    text="INFORMATION RETRIEVAL",
    font=("Arial", 8, "bold"),
    bg=SIDEBAR_COLOR,
    fg=MUTED_COLOR
)

subtitle.pack(pady=(0, 25))


# ==========================================
# 6. MAIN AREA
# ==========================================

main_area = tk.Frame(
    root,
    bg=BG_COLOR
)

main_area.pack(
    side="right",
    fill="both",
    expand=True
)


# ==========================================
# 7. HEADER
# ==========================================

header = tk.Frame(
    main_area,
    bg=WHITE,
    height=80
)

header.pack(fill="x")
header.pack_propagate(False)


header_title = tk.Label(
    header,
    text="Dashboard",
    font=("Arial", 21, "bold"),
    bg=WHITE,
    fg=TEXT_COLOR
)

header_title.pack(side="left", padx=30, pady=20)


# ==========================================
# 8. CONTENT AREA
# ==========================================

content = tk.Frame(
    main_area,
    bg=BG_COLOR
)

content.pack(
    fill="both",
    expand=True,
    padx=25,
    pady=25
)


# ==========================================
# 9. CLEAR PAGE
# ==========================================

def clear_page():

    for widget in content.winfo_children():
        widget.destroy()


# ==========================================
# 10. PAGE TITLE
# ==========================================

def create_page_title(title, description):

    tk.Label(
        content,
        text=title,
        font=("Arial", 21, "bold"),
        bg=BG_COLOR,
        fg=TEXT_COLOR
    ).pack(anchor="w", pady=(5, 8))

    tk.Label(
        content,
        text=description,
        font=("Arial", 11),
        bg=BG_COLOR,
        fg=MUTED_COLOR
    ).pack(anchor="w", pady=(0, 20))


# ==========================================
# 11. READ CSV FILE
# ==========================================

def read_csv_file(filename):

    file_path = OUTPUT_DIR / filename

    if not file_path.exists():
        return None, None

    try:

        with open(
            file_path,
            "r",
            encoding="utf-8-sig",
            newline=""
        ) as file:

            reader = csv.reader(file)

            headers = next(reader, [])
            rows = list(reader)

        return headers, rows

    except Exception as error:

        print("CSV Error:", error)

        return None, None


# ==========================================
# 12. DISPLAY TABLE
# ==========================================

def display_table(parent, headers, rows):

    # Table Container

    table_container = tk.Frame(
        parent,
        bg=WHITE
    )

    table_container.pack(
        fill="both",
        expand=True
    )

    # Search Bar

    search_frame = tk.Frame(
        table_container,
        bg=WHITE
    )

    search_frame.pack(
        fill="x",
        padx=15,
        pady=15
    )

    search_entry = ttk.Entry(
        search_frame,
        font=("Arial", 11)
    )

    search_entry.pack(
        side="left",
        fill="x",
        expand=True,
        padx=(0, 10),
        ipady=5
    )

    search_entry.insert(0, "Search in table...")

    # Records Label

    records_label = tk.Label(
        search_frame,
        text=f"Total Records: {len(rows)}",
        font=("Arial", 10, "bold"),
        bg=WHITE,
        fg=MUTED_COLOR
    )

    records_label.pack(side="right", padx=10)

    # Treeview Frame

    tree_frame = tk.Frame(
        table_container,
        bg=WHITE
    )

    tree_frame.pack(
        fill="both",
        expand=True,
        padx=15,
        pady=(0, 15)
    )

    tree = ttk.Treeview(
        tree_frame,
        columns=headers,
        show="headings"
    )

    # Configure Columns

    for column in headers:

        tree.heading(
            column,
            text=column
        )

        column_width = 650 if column == "Exact document text" else 150
        column_anchor = "w" if column == "Exact document text" else "center"

        tree.column(
            column,
            width=column_width,
            minwidth=100,
            anchor=column_anchor
        )

    # Scrollbars

    vertical_scroll = ttk.Scrollbar(
        tree_frame,
        orient="vertical",
        command=tree.yview
    )

    horizontal_scroll = ttk.Scrollbar(
        tree_frame,
        orient="horizontal",
        command=tree.xview
    )

    tree.configure(
        yscrollcommand=vertical_scroll.set,
        xscrollcommand=horizontal_scroll.set
    )

    tree.grid(
        row=0,
        column=0,
        sticky="nsew"
    )

    vertical_scroll.grid(
        row=0,
        column=1,
        sticky="ns"
    )

    horizontal_scroll.grid(
        row=1,
        column=0,
        sticky="ew"
    )

    tree_frame.rowconfigure(0, weight=1)
    tree_frame.columnconfigure(0, weight=1)

    # Insert Data

    def populate_table(data):

        tree.delete(*tree.get_children())

        for row in data:

            tree.insert(
                "",
                "end",
                values=row
            )

        records_label.config(
            text=f"Showing {len(data)} Records"
        )

    populate_table(rows)

    # Search Function

    def search_table():

        keyword = search_entry.get().strip().lower()

        if keyword == "search in table...":
            keyword = ""

        filtered_rows = [
            row for row in rows
            if any(
                keyword in str(value).lower()
                for value in row
            )
        ]

        populate_table(filtered_rows)

    def reset_search():

        search_entry.delete(0, tk.END)

        populate_table(rows)

    search_button = tk.Button(
        search_frame,
        text="Search",
        command=search_table,
        bg=ACCENT_COLOR,
        fg="#061321",
        activebackground="#00B7C2",
        activeforeground="#061321",
        relief="flat",
        padx=15,
        pady=6,
        cursor="hand2"
    )

    search_button.pack(side="left", padx=5)

    reset_button = tk.Button(
        search_frame,
        text="Reset",
        command=reset_search,
        bg="#1B2B49",
        fg=TEXT_COLOR,
        relief="flat",
        padx=15,
        pady=6,
        cursor="hand2"
    )

    reset_button.pack(side="left", padx=5)

    search_entry.bind(
        "<Return>",
        lambda event: search_table()
    )


# ==========================================
# 13. GENERAL CSV PAGE
# ==========================================

def show_csv_page(title, description, filename):

    clear_page()

    header_title.config(text=title)

    create_page_title(
        title,
        description
    )

    headers, rows = read_csv_file(filename)

    if headers is None:

        tk.Label(
            content,
            text=f"File not found or could not be read:\n{filename}",
            font=("Arial", 13),
            bg=BG_COLOR,
            fg="#FF7B8B"
        ).pack(pady=30)

        return

    if not rows:

        tk.Label(
            content,
            text="No data available in this file.",
            font=("Arial", 13),
            bg=BG_COLOR,
            fg=MUTED_COLOR
        ).pack(pady=30)

        return

    display_table(
        content,
        headers,
        rows
    )


# ==========================================
# 14. DASHBOARD PAGE
# ==========================================

def show_dashboard():

    clear_page()

    header_title.config(text="Dashboard")

    create_page_title(
        "Welcome to Information Retrieval System",
        "Search, retrieve and analyze your documents easily."
    )

    # Count Documents

    if DATASET_DIR.exists():

        document_count = len([
            file for file in DATASET_DIR.iterdir()
            if file.is_file() and file.suffix.lower() == ".txt"
        ])

    else:

        document_count = 0

    # Count Output Files

    output_count = 0

    if OUTPUT_DIR.exists():

        output_count = len([
            file for file in OUTPUT_DIR.iterdir()
            if file.is_file() and file.suffix.lower() == ".csv"
        ])

    # Dashboard Cards

    cards_frame = tk.Frame(
        content,
        bg=BG_COLOR
    )

    cards_frame.pack(
        fill="x",
        pady=20
    )

    def create_card(title, value):

        card = tk.Frame(
            cards_frame,
            bg=WHITE,
            height=130,
            highlightbackground=BORDER_COLOR,
            highlightthickness=1
        )

        card.pack(
            side="left",
            padx=8,
            expand=True,
            fill="x"
        )

        card.pack_propagate(False)

        tk.Label(
            card,
            text=title,
            font=("Arial", 12),
            bg=WHITE,
            fg=MUTED_COLOR
        ).pack(pady=(25, 5))

        tk.Label(
            card,
            text=str(value),
            font=("Arial", 25, "bold"),
            bg=WHITE,
            fg=ACCENT_COLOR
        ).pack()

    create_card(
        "Documents",
        document_count
    )

    create_card(
        "Search Methods",
        3
    )

    create_card(
        "CSV Output Files",
        output_count
    )

    # Project Information

    info_frame = tk.Frame(
        content,
        bg=WHITE,
        padx=20,
        pady=20
    )

    info_frame.pack(
        fill="x",
        pady=25
    )

    tk.Label(
        info_frame,
        text="Project Overview",
        font=("Arial", 16, "bold"),
        bg=WHITE,
        fg=TEXT_COLOR
    ).pack(anchor="w", pady=(0, 15))

    overview = [
        "Positional Index",
        "Term Frequency (TF)",
        "Document Frequency (DF)",
        "Inverse Document Frequency (IDF)",
        "TF-IDF",
        "Normalized TF-IDF",
        "Phrase Queries",
        "Cosine Similarity",
        "Boolean Queries"
    ]

    for item in overview:

        tk.Label(
            info_frame,
            text="• " + item,
            font=("Arial", 11),
            bg=WHITE,
            fg=MUTED_COLOR
        ).pack(anchor="w", pady=3)


# ==========================================
# 15. DOCUMENTS PAGE
# ==========================================

def show_documents():

    show_csv_page(
        "Documents",
        "View the original text and word count of every document.",
        "documents.csv"
    )


# ==========================================
# 15. POSITIONAL INDEX PAGE
# ==========================================

def show_positional_index():

    show_csv_page(
        "Positional Index",
        "View terms, documents and their positions.",
        "positional_index.csv"
    )


# ==========================================
# 16. TF MATRIX PAGE
# ==========================================

def show_tf_matrix():

    clear_page()

    header_title.config(text="TF Matrix")

    create_page_title(
        "Term Frequency Matrix",
        "View term frequency for each document."
    )

    headers, rows = read_csv_file(
        "term_statistics.csv"
    )

    if headers is None:

        tk.Label(
            content,
            text="term_statistics.csv not found!",
            bg=BG_COLOR,
            fg="#FF7B8B"
        ).pack(pady=30)

        return

    # Convert Long Format to Matrix

    terms = sorted(set(row[0] for row in rows))

    documents = sorted(set(row[1] for row in rows))

    matrix_headers = ["Term"] + documents

    matrix_rows = []

    for term in terms:

        term_values = [term]

        for document in documents:

            value = "0"

            for row in rows:

                if row[0] == term and row[1] == document:

                    value = row[2]
                    break

            term_values.append(value)

        matrix_rows.append(term_values)

    display_table(
        content,
        matrix_headers,
        matrix_rows
    )


# ==========================================
# 17. DF & IDF PAGE
# ==========================================

def show_df_idf():

    show_csv_page(
        "DF & IDF",
        "View Document Frequency and Inverse Document Frequency.",
        "term_statistics_summary.csv"
    )


# ==========================================
# 18. TF-IDF PAGE
# ==========================================

def show_tfidf():

    show_csv_page(
        "TF-IDF",
        "View TF-IDF values for documents and terms.",
        "tfidf.csv"
    )


# ==========================================
# 19. NORMALIZED TF-IDF PAGE
# ==========================================

def show_normalized_tfidf():

    show_csv_page(
        "Normalized TF-IDF",
        "View normalized TF-IDF values.",
        "normalized_tfidf.csv"
    )


# ==========================================
# 20. PHRASE SEARCH PAGE
# ==========================================

def show_phrase_search():

    clear_page()

    header_title.config(text="Phrase Search")

    create_page_title(
        "Phrase Query Search",
        "Find documents containing an exact phrase."
    )

    headers, rows = read_csv_file(
        "phrase_queries.csv"
    )

    if headers is None:

        tk.Label(
            content,
            text="phrase_queries.csv not found!",
            bg=BG_COLOR,
            fg="#FF7B8B"
        ).pack(pady=30)

        return

    frame = tk.Frame(
        content,
        bg=WHITE,
        padx=20,
        pady=20
    )

    frame.pack(fill="x", pady=(0, 20))

    tk.Label(
        frame,
        text="Enter Phrase:",
        font=("Arial", 12, "bold"),
        bg=WHITE
    ).pack(anchor="w", pady=5)

    phrase_entry = ttk.Entry(
        frame,
        font=("Arial", 12)
    )

    phrase_entry.pack(
        fill="x",
        pady=10,
        ipady=5
    )

    tk.Label(
        frame,
        text='Example: machine learning',
        font=("Arial", 10),
        bg=WHITE,
        fg=MUTED_COLOR
    ).pack(anchor="w")

    result_frame = tk.Frame(
        content,
        bg=BG_COLOR
    )

    result_frame.pack(
        fill="both",
        expand=True
    )

    def search_phrase():

        phrase = phrase_entry.get().strip().lower()

        if not phrase:

            return

        filtered = [
            row for row in rows
            if phrase in row[0].lower()
        ]

        for widget in result_frame.winfo_children():
            widget.destroy()

        if not filtered:

            tk.Label(
                result_frame,
                text="No matching phrase found in saved results.",
                font=("Arial", 12),
                bg=BG_COLOR,
                fg="#FF7B8B"
            ).pack(pady=20)

            return

        display_table(
            result_frame,
            headers,
            filtered
        )

    tk.Button(
        frame,
        text="Search Phrase",
        command=search_phrase,
        bg=ACCENT_COLOR,
        fg="#061321",
        activebackground="#00B7C2",
        activeforeground="#061321",
        relief="flat",
        font=("Arial", 10, "bold"),
        padx=20,
        pady=8,
        cursor="hand2"
    ).pack(anchor="w", pady=10)

    phrase_entry.bind(
        "<Return>",
        lambda event: search_phrase()
    )

    display_table(
        result_frame,
        headers,
        rows
    )


# ==========================================
# 21. SIMILARITY PAGE
# ==========================================

def show_similarity():

    show_csv_page(
        "Cosine Similarity",
        "View similarity scores between queries and documents.",
        "similarity.csv"
    )


# ==========================================
# 22. BOOLEAN SEARCH PAGE
# ==========================================

def show_boolean_search():

    clear_page()

    header_title.config(text="Boolean Search")

    create_page_title(
        "Boolean Query Search",
        "View saved results for Boolean queries."
    )

    headers, rows = read_csv_file(
        "boolean_queries.csv"
    )

    if headers is None:

        tk.Label(
            content,
            text="boolean_queries.csv not found!",
            bg=BG_COLOR,
            fg="#FF7B8B"
        ).pack(pady=30)

        return

    frame = tk.Frame(
        content,
        bg=WHITE,
        padx=20,
        pady=20
    )

    frame.pack(fill="x", pady=(0, 20))

    tk.Label(
        frame,
        text="Enter Boolean Query:",
        font=("Arial", 12, "bold"),
        bg=WHITE
    ).pack(anchor="w", pady=5)

    query_entry = ttk.Entry(
        frame,
        font=("Arial", 12)
    )

    query_entry.pack(
        fill="x",
        pady=10,
        ipady=5
    )

    tk.Label(
        frame,
        text="Example: machine learning AND data science",
        font=("Arial", 10),
        bg=WHITE,
        fg=MUTED_COLOR
    ).pack(anchor="w")

    result_frame = tk.Frame(
        content,
        bg=BG_COLOR
    )

    result_frame.pack(
        fill="both",
        expand=True
    )

    def search_boolean():

        query = query_entry.get().strip().lower()

        if not query:
            return

        filtered = [
            row for row in rows
            if query in row[0].lower()
        ]

        for widget in result_frame.winfo_children():
            widget.destroy()

        if not filtered:

            tk.Label(
                result_frame,
                text="No matching Boolean query found in saved results.",
                font=("Arial", 12),
                bg=BG_COLOR,
                fg="#FF7B8B"
            ).pack(pady=20)

            return

        display_table(
            result_frame,
            headers,
            filtered
        )

    tk.Button(
        frame,
        text="Run Boolean Search",
        command=search_boolean,
        bg=ACCENT_COLOR,
        fg="#061321",
        activebackground="#00B7C2",
        activeforeground="#061321",
        relief="flat",
        font=("Arial", 10, "bold"),
        padx=20,
        pady=8,
        cursor="hand2"
    ).pack(anchor="w", pady=10)

    query_entry.bind(
        "<Return>",
        lambda event: search_boolean()
    )

    display_table(
        result_frame,
        headers,
        rows
    )


# ==========================================
# 23. SIDEBAR BUTTONS
# ==========================================

def create_sidebar_button(text, command):

    button = tk.Button(
        sidebar,
        text=text,
        font=("Arial", 10, "bold"),
        bg=SIDEBAR_COLOR,
        fg=TEXT_COLOR,
        activebackground="#123A50",
        activeforeground=ACCENT_COLOR,
        relief="flat",
        anchor="w",
        padx=20,
        pady=13,
        bd=0,
        highlightthickness=0,
        cursor="hand2",
        command=command
    )

    button.pack(
        fill="x",
        padx=10,
        pady=2
    )

    return button


# ==========================================
# 24. NAVIGATION
# ==========================================

create_sidebar_button(
    "Dashboard",
    show_dashboard
)

create_sidebar_button(
    "Documents",
    show_documents
)

create_sidebar_button(
    "Positional Index",
    show_positional_index
)

create_sidebar_button(
    "TF Matrix",
    show_tf_matrix
)

create_sidebar_button(
    "DF & IDF",
    show_df_idf
)

create_sidebar_button(
    "TF-IDF",
    show_tfidf
)

create_sidebar_button(
    "Normalized TF-IDF",
    show_normalized_tfidf
)

create_sidebar_button(
    "Phrase Search",
    show_phrase_search
)

create_sidebar_button(
    "Similarity",
    show_similarity
)

create_sidebar_button(
    "Boolean Search",
    show_boolean_search
)


# ==========================================
# 25. START APPLICATION
# ==========================================

show_dashboard()

root.mainloop()