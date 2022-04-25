import streamlit as st
from multiapp import MultiApp
import EDMMapper
import EDMGrapher
import EDMPredicter
import home
st.set_page_config(layout="wide")


apps = MultiApp()

# Add all your application here
apps.add_app("Home", home.app)
apps.add_app("Overflows Map", EDMMapper.app)
apps.add_app("Spill charts",  EDMGrapher.app)
apps.add_app("River and Rainfall", EDMPredicter.app)

# The main app
apps.run()
