import pandas as pd
import warnings
import numpy as np
import json
import os


class Dataset_Processor:
    def __init__(self, data_source_method, persistance_method):
        self.data_source = data_source_method
        self.persistance = persistance_method

    def get_data(self, path):
        if self.data_source == 'Local':
            self.dataset = pd.read_csv(path)
        
        return self.dataset
    
    def set_data(self,dataset):
        self.dataset = dataset
        return dataset

    def describe_dataset(self):
        print(self.dataset.head())
    
    def set_meta_cols(self,meta_cols):
        self.meta_cols = meta_cols

    def set_rating_cols(self,rating_cols, ratings_given):
        self.rating_cols = rating_cols
        self.ratings_given = ratings_given
    
    def set_pid_uid_title_price_cols(self,pid_col_name, uid_col_name, title_col_name, price_col_name):
        self.pid_col = pid_col_name
        self.uid_col = uid_col_name
        self.title_col = title_col_name
        self.price_col = price_col_name
    
    def clean_dataset(self):
        
        all_features = self.meta_cols + self.rating_cols
        cleaned_dataset = self.dataset.dropna(axis=0, subset=all_features)
        self.cleaned_dataset = cleaned_dataset
    
    def process_dataset(self):

        meta_dataset = self.cleaned_dataset[self.meta_cols]
        meta_dataset = meta_dataset.rename(columns = {self.pid_col: 'asin', self.title_col: 'title', self.price_col: 'price'})
        print(self.pid_col)
        print(meta_dataset.columns) 
        meta_dataset.drop_duplicates(subset =['asin'], keep = False, inplace = True)  
        self.meta_dataset = meta_dataset

        rating_dataset = self.cleaned_dataset[self.rating_cols]
        rating_dataset.drop_duplicates(subset =[self.uid_col,self.pid_col], keep = False, inplace = True) 

        if self.ratings_given:
            pass
        
        else:
            # rating_dataset['rating']=1  
            rating_dataset = rating_dataset.assign(rating=pd.Series(np.ones(rating_dataset.shape[0])).values)

        rating_dataset = rating_dataset.rename(columns={self.uid_col: "userId",self.pid_col:'asin'})
        self.rating_dataset = rating_dataset

    def sanity_check(self):
        print('dataset shape...', self.dataset.shape)
        print('cleaned dataset shape...', self.cleaned_dataset.shape)
        print('meta dataset shape...', self.meta_dataset.shape)
        print('rating dataset shape...', self.rating_dataset.shape)

    
    def set_feature_types(self, categorical_features, w2v_features, numerical_features):

        self.features = {'categorical_features': categorical_features, 
                         'w2v_features': w2v_features,
                         'numerical_features': numerical_features
                        }

    def dataset_persistance(self, path):
        if self.persistance == 'Local':
            self.rating_dataset.to_csv(path + "ratings.csv", index = False)
            self.meta_dataset.to_csv(path + "product_meta.csv", index = False)

            with open( path + 'features.json', 'w') as fp:
                
                json.dump(self.features, fp,indent=4)
    
    def get_result_files(self):
        return self.rating_dataset, self.meta_dataset, self.features
    

     

# data_processor = Dataset_Processor(data_source_method='Local', persistance_method='Local')
# data_processor.get_data('/home/thulith/Desktop/Trabeya/rec-sys/datasets/Bike_Exchange/purchasing.csv')
# data_processor.set_meta_cols(['advert identifier','category','brand','advert title','sale price'])
# data_processor.set_rating_cols(['customer identifier','advert identifier'], ratings_given=False)
# data_processor.set_pid_uid_title_price_cols(pid_col_name="advert identifier", 
#                                             uid_col_name="customer identifier", 
#                                             title_col_name='advert title', 
#                                             price_col_name='sale price')

# data_processor.clean_dataset()
# data_processor.process_dataset()
# data_processor.sanity_check()

# data_processor.set_feature_types(categorical_features='None', w2v_features={"title":100, "category":100}, numerical_features="None")

# data_processor.dataset_persistance('/home/thulith/Desktop/Trabeya/rec-sys/datasets/Bike_Exchange_Preproc/')













