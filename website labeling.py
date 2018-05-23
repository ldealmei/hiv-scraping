
# coding: utf-8

# - webview.create_window(title, url, width=800, height=600, resizable=True, fullscreen=False)
# 
# Create a new WebView window. Calling this function will block execution, so you have to execute your program logic in a separate thread.
#  - webview.load_url(url)
# 
# Load a new URL into a previously created WebView window. This function must be invoked after WebView windows is created with create_window(). Otherwise an exception is thrown.

# In[1]:


import threading
import time
import webview
import pandas as pd

class webThread(threading.Thread):
    def __init__(self, threadID, name, limit = 50):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        
        try :
            # fetch most recent version
            d = pd.read_csv('dom_lbl.csv')
            self.urls = d[d['hiv'].isnull()]['domain'][:limit]
            self.past_df = d.copy()
        
        except :
            # start from 0
            d = pd.read_csv('domains.csv')
            self.urls = d['domain'][:limit]
            self.past_df = d.copy()

        self.lbls = ['hiv','health','gov','edu','ngo']
        # init dataframe
        self.data_lbl = d.copy()
        for lbl in self.lbls :
            self.data_lbl[lbl] = None
        
    def run(self):
        for idx,url in self.urls.iteritems() :
            print "Starting " + self.name + " on " + url
            webview.load_url(url)
            print 'Getting input...'
            for lbl in self.lbls :
                while True : #ask for input while the user makes mistakes
                    try :
                        rep = input(lbl + '?')
                        if rep not in [0,1]:
                            raise
                        self.data_lbl.loc[idx,lbl] = rep
                    except :
                        print "Please enter 0 or 1"
                    else :
                        break

            self.past_df.update(self.data_lbl, overwrite = False)
            self.past_df.to_csv('dom_lbl.csv', index=False)

# Create new threads
web_thread = webThread(1, "Web Thread")

# Start new Threads
web_thread.start()

webview.create_window('Please label this site', width=1200, height=800, resizable=True, fullscreen=False)


# # Quick labeling of recuring websites

# In[ ]:


# import pandas as pd
# d = pd.read_csv('domains.csv')
# lbls = ['hiv','health','gov','edu','ngo']
# # init dataframe
# for lbl in lbls :
#     d[lbl] = None


# In[ ]:


# for lbl in lbls :
#     d.loc[d['domain'].str.contains('.iu.edu'),lbl] = 0
# d.loc[d['domain'].str.contains('.iu.edu'),'edu'] = 1


# In[ ]:


# d.to_csv('dom_lbl.csv',index=False)


# In[ ]:


# d

