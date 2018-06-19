
# coding: utf-8

# In[ ]:


import pandas as pd
sites=pd.read_csv("mysites.csv")


# In[ ]:


# Import the necessary modules
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split 
from sklearn.pipeline import Pipeline
from sklearn.metrics import precision_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import roc_auc_score
from sklearn.metrics import accuracy_score
from sklearn.model_selection import cross_val_score
from sklearn.metrics import classification_report
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import roc_auc_score
from sklearn.metrics import roc_curve, auc
from sklearn.metrics import cohen_kappa_score
from sklearn.metrics import precision_recall_curve
import numpy as np


# In[ ]:


# Create the token pattern: TOKENS_ALPHANUMERIC
TOKENS_ALPHANUMERIC = '[A-Za-z0-9]+(?=\\s+)' 


# In[ ]:


# Create a Series to store the labels: y
y = sites.hiv
#print(y.head(), "\n")

# Create X (features dataset)
X= sites.all_text
#print(X.head())

# Create training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=72)


# In[ ]:


# Instantiate Pipeline object with the best params from the grid search: pl_sgdc
pl_sgdc = Pipeline([
    ('vect', CountVectorizer(stop_words="english", token_pattern=TOKENS_ALPHANUMERIC, max_df=0.5, 
                             max_features=50000, ngram_range=(1,1))),
    ('tfidf', TfidfTransformer(norm="l1", use_idf=True)),
    ('clf', SGDClassifier(alpha= 1e-05, max_iter=10, class_weight="balanced", penalty= 'elasticnet'))])

# Fit the Pipeline to the training data
predicted= pl_sgdc.fit(X_train, y_train)

