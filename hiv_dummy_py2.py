
# coding: utf-8

# In[2]:


import pandas as pd

# Read the websites data
sites=pd.read_csv("hiv_sites.csv")


# In[4]:


# Import the necessary modules
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split 
from sklearn.metrics import confusion_matrix
from sklearn.metrics import roc_auc_score
from sklearn.metrics import accuracy_score
from sklearn.model_selection import cross_val_score
from sklearn.metrics import classification_report


# In[5]:


# Create a Series to store the labels: y
y = sites.HIV_related
#print(y.head(), "\n")

# Create X (features dataset)
X= sites.text_dump
#print(X.head())

# Create training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=73)


# In[8]:


# Lemmatizer for CountVectorizer

# Create a Lemmatizer object to pass in the sklearn Countvectorizer as `tokenizer` argument
# Remove non-alphabetic characters,
# Remove stop words
# Lemmatize
# Import the necessary nltk modules 
from nltk import word_tokenize          
from nltk.stem import WordNetLemmatizer 

# Create the LemmaTokenizer Object in order to pass in CountVectorizer
class LemmaTokenizer(object):
    # Initilize WordNetLemmatizer in the class
    def __init__(self):
        self.lemmatizer = WordNetLemmatizer()
        
    def __call__(self, doc):
        # Tokenize the doc: token_list
        tokens_list=word_tokenize(doc)
        
       # Create a list by iterating over tokens_list and retaining only alphabetical tokens
        # Use the .isalpha() method to check if the token is alphabetical and its length is greater than 1
        alpha_token_list= [token for token in tokens_list if token.isalpha() and len(token)>1]
                
        # Return the list after iterating over alpha_tokens_list and lemmatize each token
        return [self.lemmatizer.lemmatize(t) for t in alpha_token_list]


# In[6]:


# Create a Dummy Classifier

import numpy as np
from sklearn.base import BaseEstimator
from sklearn.base import ClassifierMixin

class DummyEstimator(BaseEstimator, ClassifierMixin) :
    def __init__(self, key1, key2, key3):
        self.key1= key1
        self.key2= key2
        self.key3= key3
        
    def fit(self, X, y=None):

        return self 
                                       
                                
    def predict(self, X, y=None):
        # predict() takes X_train or X_test and returns a list of predictions with the values 1 or 0
        
        # accumulate the predictions in the pred_lst
        pred_lst=[]  
        # iterate over the rows of the X dataframe
        # append 1 or 0 regarding the keywords conditions of the sliced rows(related values of each row)
        for index, row in X.iterrows():
            if (row[self.key1]>0 and row[self.key2]>0):
                pred_lst.append(1)
                
            elif (row[self.key1]>0 and row[self.key3]>0):
                 pred_lst.append(1)
                   
            else : pred_lst.append(0)
            
        # return occurence_list  
        return (pred_lst)
    

    def score(self, X, y=None):
        #counts the value of (Number of the correct predictions / Total predictions)
                
        # Zip the X(y_test) and preds
        # Eventhough X is given as a Series and preds is a list, zip() function creates tuples from them
        tuple_lst= list(zip(X, preds))
        prediction_lst=[]
        # unpack the zipped tuples and iterate over the list checking
        # if the prediction and the real values are equal
        for real, prediction in tuple_lst:
            if real==prediction:
                prediction_lst.append(1)
            else: 
                prediction_lst.append(0)
        # return the prediction score
        return (sum(prediction_lst)/ len(prediction_lst))


# In[10]:


# Create a pipeline with Dummy Classifier

# Import pipeline
from sklearn.pipeline import Pipeline

# Instantiate Pipeline object: pl
# Add the 'CountVectorizer' step (with the name 'vec') to the correct position in the pipeline
pl = Pipeline([
               ('vec', CountVectorizer(stop_words='english', tokenizer=LemmaTokenizer())),
               ('clf', DummyEstimator("hiv", "aid", "health"))
    ])

# Fit the Pipeline to the training data
pl.fit(X_train, y_train)

