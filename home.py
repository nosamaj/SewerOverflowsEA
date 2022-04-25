from modulefinder import Module
import streamlit as st

def app():
    
    st.title("Overflow and EA River Data in England and Wales")

    st.markdown(
        """
        This application summarises Event Duration Monitor (EDM) data and Enironment Agency
        gauge data from their Flooding API.

        The app has three sections:
        - An interactive map showing EA return data 
        - Interactive plots describing EA return data
        - Interacive graph showing an example of river level precition using an LSTM neural network  

        The data for the project is freely available [add a link]. Any conclusions drawn from the data
        are that of the Author and do not repesent any organisation to which they are affiliated. 

        """
    )