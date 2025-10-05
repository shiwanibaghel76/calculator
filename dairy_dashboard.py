# 🥛 Dairy Dashboard (Streamlit)
# Author: ChatGPT
# Description: End-to-end mini MIS for a small dairy:
#   - Ratio Fat/SNF Settings
#   - Price Calculator (rate & amount)
#   - Customer master (CRUD)
#   - Milk collection Data Entry
#   - Customer Reports + CSV export
#
# How to run:
#   1) pip install streamlit pandas
#   2) streamlit run dairy_dashboard.py

import os
import sqlite3
from contextlib import closing
from datetime import date
from typing import Optional, List, Tuple, Any

import pandas as pd
import streamlit as st

DB_PATH = os.path.join(os.path.dirname(__file__), "dairy.db")


# ------------------------- DB Helpers -------------------------

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with closing(get_conn()) as conn, conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            base_fat REAL NOT NULL DEFAULT 3.5,
            base_snf REAL NOT NULL DEFAULT 8.5,
            base_rate REAL NOT NULL DEFAULT 30.0,   -- base price per liter
            fat_rate REAL NOT NULL DEFAULT 4.0,     -- ₹ per +1.0% fat over base (negative if below)
            snf_rate REAL NOT NULL DEFAULT 2.0      -- ₹ per +1.0% snf over base (negative if below)
        );
        """)
        conn.execute("INSERT OR IGNORE INTO settings (id) VALUES (1);")

        conn.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            phone TEXT,
            address TEXT,
            notes TEXT
        );
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_date TEXT NOT NULL,       -- ISO date yyyy-mm-dd
            customer_id INTEGER NOT NULL,
            qty_liters REAL NOT NULL,
            fat REAL NOT NULL,
            snf REAL NOT NULL,
            rate REAL NOT NULL,             -- computed per liter
            amount REAL NOT NULL,           -- qty_liters * rate
            notes TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        );
        """)


def fetch_settings() -> sqlite3.Row:
    with closing(get_conn()) as conn, conn:
        return conn.execute("SELECT * FROM settings WHERE id = 1").fetchone()


def update_settings(base_fat: float, base_snf: float, base_rate: float, fat_rate: float, snf_rate: float):
    with closing(get_conn()) as conn, conn:
        conn.execute("""
            UPDATE settings
               SET base_fat=?, base_snf=?, base_rate=?, fat_rate=?, snf_rate=?
             WHERE id = 1
        """, (base_fat, base_snf, base_rate, fat_rate, snf_rate))


def upsert_customer(name: str, phone: Optional[str], address: Optional[str], notes: Optional[str], customer_id: Optional[int] = None) -> int:
    with closing(get_conn()) as conn, conn:
        if customer_id:
            conn.execute("""
                UPDATE customers
                   SET name=?, phone=?, address=?, notes=?
                 WHERE id=?
            """, (name.strip(), (phone or "").strip(), (address or "").strip(), (notes or "").strip(), customer_id))
            return customer_id
        cur = conn.execute("""
            INSERT INTO customers (name, phone, address, notes)
            VALUES (?, ?, ?, ?)
        """, (name.strip(), (phone or "").strip(), (address or "").strip(), (notes or "").strip()))
        return cur.lastrowid


def delete_customer(customer_id: int):
    with closing(get_conn()) as conn, conn:
        cnt = conn.execute("SELECT COUNT(*) AS c FROM entries WHERE customer_id=?", (customer_id,)).fetchone()["c"]
        if cnt > 0:
            raise ValueError("Cannot delete: milk collection entries exist for this customer.")
        conn.execute("DELETE FROM customers WHERE id=?", (customer_id,))


def list_customers() -> pd.DataFrame:
    with closing(get_conn()) as conn, conn:
        return pd.read_sql_query("SELECT id, name, phone, address, notes FROM customers ORDER BY name", conn)


def add_entry(entry_date: date, customer_id: int, qty_liters: float, fat: float, snf: float, rate: float, amount: float, notes: Optional[str] = ""):
    with closing(get_conn()) as conn, conn:
        conn.execute("""
        INSERT INTO entries (entry_date, customer_id, qty_liters, fat, snf, rate, amount, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (entry_date.isoformat(), customer_id, qty_liters, fat, snf, rate, amount, (notes or "").strip()))


def query_entries(customer_id: Optional[int], start_date: Optional[date], end_date: Optional[date]) -> pd.DataFrame:
    q = ("SELECT e.id, e.entry_date, c.name AS customer, e.qty_liters, e.fat, e.snf, e.rate, e.amount, e.notes "
         "FROM entries e JOIN customers c ON e.customer_id=c.id WHERE 1=1 ")
    params: List[Any] = []
    if customer_id:
        q += " AND e.customer_id=?"
        params.append(customer_id)
    if start_date:
        q += " AND e.entry_date>=?"
        params.append(start_date.isoformat())
    if end_date:
        q += " AND e.entry_date<=?"
        params.append(end_date.isoformat())
    q += " ORDER BY e.entry_date ASC, c.name ASC"

    with closing(get_conn()) as conn, conn:
        return pd.read_sql_query(q, conn, params=params)


# ------------------------- Business Logic -------------------------

def compute_rate_and_amount(fat: float, snf: float, qty_liters: float, srow: sqlite3.Row) -> Tuple[float, float]:
    base_rate = float(srow["base_rate"])
    base_fat = float(srow["base_fat"])
    base_snf = float(srow["base_snf"])
    fat_rate = float(srow["fat_rate"])
    snf_rate = float(srow["snf_rate"])

    rate = base_rate + (fat - base_fat) * fat_rate + (snf - base_snf) * snf_rate
    rate = round(rate, 2)
    amount = round(max(0.0, rate) * qty_liters, 2)
    return rate, amount


def snf_from_lr(lr: float, temp_c: float, fat: float) -> float:
    """Approximate SNF from Lactometer Reading and temperature.
       CLR = LR + (Temp_C - 27) * 0.2
       SNF ≈ (CLR/4) + (0.21 * Fat) + 0.36
    """
    clr = lr + (temp_c - 27.0) * 0.2
    snf = (clr / 4.0) + (0.21 * fat) + 0.36
    return round(snf, 2)


# ------------------------- UI -------------------------

def page_settings():
    st.subheader("⚙️ Ratio / Fat–SNF Settings")
    st.caption("These settings drive the per-liter price calculation for all entries unless overridden in code.")

    s = fetch_settings()
    with st.form("settings_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            base_fat = st.number_input("Base Fat (%)", min_value=0.0, max_value=12.0, value=float(s['base_fat']), step=0.1)
            base_snf = st.number_input("Base SNF (%)", min_value=0.0, max_value=12.0, value=float(s['base_snf']), step=0.1)
        with c2:
            base_rate = st.number_input("Base Rate (₹/L)", min_value=0.0, max_value=999.0, value=float(s['base_rate']), step=0.5)
            fat_rate = st.number_input("Fat Rate (₹ per +1.0% Fat)", min_value=-99.0, max_value=99.0, value=float(s['fat_rate']), step=0.5)
        with c3:
            snf_rate = st.number_input("SNF Rate (₹ per +1.0% SNF)", min_value=-99.0, max_value=99.0, value=float(s['snf_rate']), step=0.5)

        submitted = st.form_submit_button("Save Settings", use_container_width=True)
        if submitted:
            update_settings(base_fat, base_snf, base_rate, fat_rate, snf_rate)
            st.success("Settings saved ✅")


def page_calculator():
    st.subheader("🧮 Price Calculator (Fat/SNF)")

    srow = fetch_settings()
    st.info(
        f"Current Pricing: rate = {srow['base_rate']} + (fat − {srow['base_fat']})×{srow['fat_rate']}"
        f" + (snf − {srow['base_snf']})×{srow['snf_rate']} (₹/L)"
    )

    c1, c2 = st.columns(2)

    with c1:
        st.markdown("**Direct Fat/SNF Input**")
        fat = st.number_input("Fat (%)", min_value=0.0, max_value=12.0, step=0.1, value=4.0, key="calc_fat")
        snf = st.number_input("SNF (%)", min_value=0.0, max_value=12.0, step=0.1, value=8.5, key="calc_snf")
        qty = st.number_input("Quantity (Liters)", min_value=0.0, max_value=2000.0, step=0.5, value=10.0, key="calc_qty")
        if st.button("Calculate", use_container_width=True):
            rate, amount = compute_rate_and_amount(fat, snf, qty, srow)
            st.success(f"Rate: ₹ {rate:.2f} per L   |   Amount: ₹ {amount:.2f}")

    with c2:
        st.markdown("**Helper: Estimate SNF from LR + Temperature**")
        with st.expander("Open SNF Helper"):
            lr = st.number_input("Lactometer Reading (LR)", min_value=0.0, max_value=200.0, step=0.5, value=30.0)
            temp_c = st.number_input("Milk Temperature (°C)", min_value=0.0, max_value=60.0, step=0.5, value=27.0)
            fat_for_snf = st.number_input("Fat (%) for SNF estimate", min_value=0.0, max_value=12.0, step=0.1, value=4.0)
            if st.button("Estimate SNF", key="snf_btn", use_container_width=True):
                snf_est = snf_from_lr(lr, temp_c, fat_for_snf)
                st.info(f"Estimated SNF: **{snf_est}%** (approximation)")


def page_customers():
    st.subheader("👤 Customer Master")

    st.markdown("**Add or Edit Customer**")
    df = list_customers()
    names = ["(New)"] + df["name"].tolist()
    selection = st.selectbox("Select customer to edit", names, index=0)
    if selection != "(New)":
        row = df[df["name"] == selection].iloc[0]
        cust_id = int(row["id"])
        default_name = row["name"]
        default_phone = row["phone"]
        default_address = row["address"]
        default_notes = row["notes"]
    else:
        cust_id = None
        default_name = ""
        default_phone = ""
        default_address = ""
        default_notes = ""

    with st.form("cust_form", clear_on_submit=False):
        c1, c2 = st.columns(2)
        with c1:
            name = st.text_input("Name *", value=default_name)
            phone = st.text_input("Phone", value=default_phone)
        with c2:
            address = st.text_area("Address", value=default_address)
            notes = st.text_area("Notes", value=default_notes, height=90)

        colA, colB, _ = st.columns([1, 1, 2])
        submitted = colA.form_submit_button("Save", use_container_width=True)
        delete_clicked = colB.form_submit_button("Delete", use_container_width=True, disabled=(cust_id is None))

        if submitted:
            if not name.strip():
                st.error("Name is required.")
            else:
                try:
                    returned_id = upsert_customer(name, phone, address, notes, customer_id=cust_id)
                    st.success(f"Saved ✅ (ID: {returned_id})")
                except sqlite3.IntegrityError as e:
                    st.error(f"Error: {str(e)}")

        if delete_clicked:
            try:
                delete_customer(int(cust_id))
                st.success("Deleted ✅")
            except Exception as e:
                st.error(f"Error: {str(e)}")

    st.divider()
    st.markdown("**All Customers**")
    st.dataframe(df, use_container_width=True, hide_index=True)


def page_entry():
    st.subheader("📝 Milk Collection – Data Entry")

    srow = fetch_settings()
    customers_df = list_customers()
    if customers_df.empty:
        st.warning("No customers yet. Please add a customer in the **Customers** tab.")
        return

    c1, c2, c3 = st.columns(3)
    with c1:
        entry_date = st.date_input("Date", value=date.today())
        cust_name = st.selectbox("Customer", customers_df["name"].tolist())
        customer_id = int(customers_df[customers_df["name"] == cust_name]["id"].iloc[0])
    with c2:
        qty = st.number_input("Quantity (Liters)", min_value=0.0, max_value=2000.0, step=0.5, value=10.0)
        fat = st.number_input("Fat (%)", min_value=0.0, max_value=12.0, step=0.1, value=4.0)
    with c3:
        snf = st.number_input("SNF (%)", min_value=0.0, max_value=12.0, step=0.1, value=8.5)
        notes = st.text_input("Notes (optional)")

    rate, amount = compute_rate_and_amount(fat, snf, qty, srow)
    st.info(f"Computed Rate: **₹ {rate:.2f}/L**   |   Amount: **₹ {amount:.2f}**")

    if st.button("Save Entry", use_container_width=True):
        if qty <= 0:
            st.error("Quantity must be positive.")
        else:
            add_entry(entry_date, customer_id, qty, fat, snf, rate, amount, notes)
            st.success("Entry saved ✅")

    st.divider()
    st.markdown("**Recent Entries (last 30 days)**")
    last_30 = date.fromordinal(date.today().toordinal() - 30)
    df = query_entries(None, last_30, date.today())
    st.dataframe(df, use_container_width=True, hide_index=True)


def page_reports():
    st.subheader("📊 Customer Reports")

    customers_df = list_customers()
    customer_options = ["All"] + customers_df["name"].tolist()
    c1, c2, c3, _ = st.columns(4)
    with c1:
        customer_name = st.selectbox("Customer", customer_options)
        customer_id = None
        if customer_name != "All":
            customer_id = int(customers_df[customers_df["name"] == customer_name]["id"].iloc[0])
    with c2:
        start_date = st.date_input("From", value=date(date.today().year, date.today().month, 1))
    with c3:
        end_date = st.date_input("To", value=date.today())

    df = query_entries(customer_id, start_date, end_date)

    if df.empty:
        st.warning("No records found for the selected filters.")
        return

    total_liters = float(df["qty_liters"].sum())
    avg_fat = float((df["fat"] * df["qty_liters"]).sum() / total_liters) if total_liters else 0.0
    avg_snf = float((df["snf"] * df["qty_liters"]).sum() / total_liters) if total_liters else 0.0
    total_amount = float(df["amount"].sum())

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Liters", f"{total_liters:.2f}")
    m2.metric("Avg Fat (%)", f"{avg_fat:.2f}")
    m3.metric("Avg SNF (%)", f"{avg_snf:.2f}")
    m4.metric("Total Amount (₹)", f"{total_amount:.2f}")

    st.divider()
    st.markdown("**Detailed Entries**")
    st.dataframe(df, use_container_width=True, hide_index=True)

    df_daily = (
        df.assign(entry_date=pd.to_datetime(df["entry_date"]))
          .groupby("entry_date", as_index=False)
          .agg(total_liters=("qty_liters", "sum"),
               avg_fat=("fat", "mean"),
               avg_snf=("snf", "mean"),
               total_amount=("amount", "sum"))
    )
    st.markdown("**Daily Summary**")
    st.dataframe(df_daily, use_container_width=True, hide_index=True)

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download Entries CSV", data=csv_bytes, file_name="dairy_entries.csv", mime="text/csv", use_container_width=True)

    csv2_bytes = df_daily.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download Daily Summary CSV", data=csv2_bytes, file_name="dairy_daily_summary.csv", mime="text/csv", use_container_width=True)


def main():
    st.set_page_config(page_title="Dairy Dashboard", page_icon="🥛", layout="wide")
    st.title("🥛 Dairy Dashboard")
    st.caption("Fat/SNF ratio settings, calculator, customer master, data entry & reports — all in one simple app.")
    init_db()

    with st.sidebar:
        st.header("Navigate")
        page = st.radio("Go to", ["Settings", "Calculator", "Customers", "Data Entry", "Reports"], label_visibility="collapsed")
        st.markdown("---")
        st.caption("Tip: Configure your base pricing in **Settings** first.")

    if page == "Settings":
        page_settings()
    elif page == "Calculator":
        page_calculator()
    elif page == "Customers":
        page_customers()
    elif page == "Data Entry":
        page_entry()
    elif page == "Reports":
        page_reports()


if __name__ == "__main__":
    main()
