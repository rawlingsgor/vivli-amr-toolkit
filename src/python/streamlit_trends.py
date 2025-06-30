#!/usr/bin/env python3
import streamlit as st
from PIL import Image

st.set_page_config(page_title="AMR MIC Trends", layout="wide")
st.title("Tigecycline MIC Percentile Curves (2004-2023)")

plots = {
    # display title                         : relative file path
    "Acinetobacter junii – Tigecycline"     : "plots/Acinetobacter_junii_Tigecycline.png",
    "Acinetobacter lwoffii – Tigecycline"   : "plots/Acinetobacter_lwoffii_Tigecycline.png",
    "Citrobacter freundii – Tigecycline"    : "plots/Citrobacter_freundii_Tigecycline.png",
    "Enterococcus faecalis – Tigecycline"   : "plots/Enterococcus_faecalis_Tigecycline.png",
    "Enterococcus faecium – Tigecycline"    : "plots/Enterococcus_faecium_Tigecycline.png",
    "Escherichia coli – Tigecycline"        : "plots/Escherichia_coli_Tigecycline.png",
    "Escherichia coli – Tigecycline (p-pct)": "plots/Escherichia_coli_Tigecycline_percentiles.png",
    "Haemophilus influenzae – Tigecycline"  : "plots/Haemophilus_influenzae_Tigecycline.png",
    "Klebsiella aerogenes – Tigecycline"    : "plots/Klebsiella_aerogenes_Tigecycline.png",
    "Staphylococcus aureus – Tigecycline"   : "plots/Staphylococcus_aureus_Tigecycline.png",
    "Streptococcus pneumoniae – Tigecycline": "plots/Streptococcus_pneumoniae_Tigecycline.png",
}

choice = st.selectbox("Select organism–drug combo:", list(plots.keys()))
img = Image.open(plots[choice])
st.image(img, use_column_width=True)
