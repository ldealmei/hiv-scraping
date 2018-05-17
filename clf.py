from nltk import word_tokenize
from nltk.stem import WordNetLemmatizer


# Create the LemmaTokenizer Object in order to pass in CountVectorizer
class LemmaTokenizer(object):
    # Initilize WordNetLemmatizer in the class
    def __init__(self):
        self.lemmatizer = WordNetLemmatizer()

    def __call__(self, doc):
        # Tokenize the doc: token_list
        tokens_list = word_tokenize(doc)

        # Create a list by iterating over tokens_list and retaining only alphabetical tokens
        # Use the .isalpha() method to check if the token is alphabetical and its length is greater than 1
        alpha_token_list = [token for token in tokens_list if token.isalpha() and len(token) > 1]

        # Return the list after iterating over alpha_tokens_list and lemmatize each token
        return [self.lemmatizer.lemmatize(t) for t in alpha_token_list]

