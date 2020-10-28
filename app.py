from dataset_prep import Dataset_Processor
import streamlit as st 
import pandas as pd
import bokeh
import bokeh.models
import base64

"""
## Nabu Dataset Processor

this application allows you to do this..

Author: [Thulith Edirisinghe] (https://github.com/Thulith95)
Source: [Streamlit on ACR] (https://medium.com/@saitracychen/deploy-a-streamlit-app-to-azure-126452e7df6d)
"""

def main():
        
    st.set_option('deprecation.showfileUploaderEncoding', False)

    uploaded_file = st.file_uploader("Choose a file")
    if uploaded_file is not None:
        uploaded_dataset = pd.read_csv(uploaded_file)

    if not uploaded_file:
        return



    data_processor = Dataset_Processor(data_source_method='Local', persistance_method='Local')
    dataset = data_processor.set_data(uploaded_dataset)

    process_dataset = False
    # extract col names
    dataset_columns = dataset.columns

    # set rating col names
    # ['customer identifier','advert identifier']
    uid_col = st.sidebar.selectbox('Select User ID col', dataset_columns)
    pid_col = st.sidebar.selectbox('Select Product ID col', dataset_columns)
    
    # set title and price col names
    # ['advert title', 'sale price']
    title_col = st.sidebar.selectbox('Select title col', dataset_columns)
    sale_price_col = st.sidebar.selectbox('Select price col', dataset_columns)
    
    # set meta col names 
    # ['advert identifier','category','brand','advert title','sale price']
    meta_cols = st.sidebar.multiselect('Meta data columns', dataset_columns)

    if st.sidebar.button('Process Dataset'):
        process_dataset = True
    
    if not process_dataset:
        return
    
    data_processor.set_meta_cols(meta_cols)
    data_processor.set_rating_cols([uid_col, pid_col], ratings_given=False)
    data_processor.set_pid_uid_title_price_cols(pid_col_name=str(pid_col), 
                                                uid_col_name=str(uid_col), 
                                                title_col_name=title_col, 
                                                price_col_name=sale_price_col)
    data_processor.clean_dataset()
    data_processor.process_dataset()
    
    data_processor.set_feature_types(categorical_features='None', w2v_features={"title":100, "category":100}, numerical_features="None")

    # data_processor.dataset_persistance('/home/thulith/Desktop/Trabeya/rec-sys/datasets/Bike_Exchange_Preproc/')

    rating_dataset, meta_dataset, _= data_processor.get_result_files()

    rating_csv = rating_dataset.to_csv(index=False)
    meta_csv = meta_dataset.to_csv(index=False)



    rating_b64 = base64.b64encode(rating_csv.encode()).decode()  # some strings <-> bytes conversions necessary here
    meta_b64 = base64.b64encode(meta_csv.encode()).decode()  # some strings <-> bytes conversions necessary here

    rating_href = f'<a href="data:file/csv;base64,{rating_b64}">Download ratings.csv File</a> (right-click and save as &lt;some_name&gt;.csv)'
    meta_href = f'<a href="data:file/csv;base64,{meta_b64}">Download metadata.csv File</a> (right-click and save as &lt;some_name&gt;.csv)'
    
    st.write("rating dataset", rating_dataset)
    st.markdown(rating_href, unsafe_allow_html=True)

    st.write("meta dataset", meta_dataset)
    st.markdown(meta_href, unsafe_allow_html=True)




if __name__ == "__main__":
    main()
