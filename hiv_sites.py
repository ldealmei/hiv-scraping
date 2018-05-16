
# coding: utf-8

# In[19]:


import pandas as pd

# Read the websites data
sites=pd.read_csv("hiv_sites.csv")


# ## Lemmatizer for CountVectorizer
# 
# Create a Lemmatizer object to pass in the Sklearn Countvectorizer as `tokenizer` argument
# - Remove **non-alphabetic** characters,
# - **Remove stop words** 
# - **Lemmatize**

# In[20]:


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


# In[29]:


# Import the necessary modules
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split 
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline


# In[26]:


# Initialize a CountVectorizer object: count_vectorizer
# Specify the argument stop_words="english" so that stop words are removed
# Pass in LemmaTokenizer() as an argument
count_vectorizer = CountVectorizer(stop_words='english', tokenizer=LemmaTokenizer())


# In[27]:


# Create a Series to store the labels: y
y = sites.HIV_related
#print(y.head(), "\n")

# Create X (features dataset)
X= sites.text_dump
#print(X.head())

# Create training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=73)


# ## Create the Pipeline

# In[ ]:


# Instantiate Pipeline object: pl
# Add the 'CountVectorizer' step (with the name 'vec') to the correct position in the pipeline
pl = Pipeline([
               ('vec', CountVectorizer(stop_words='english', tokenizer=LemmaTokenizer())),
               ('clf', MultinomialNB())
    ])

# Fit the Pipeline to the training data
pl.fit(X_train, y_train)

