
# coding: utf-8

# - webview.create_window(title, url, width=800, height=600, resizable=True, fullscreen=False)
# 
# Create a new WebView window. Calling this function will block execution, so you have to execute your program logic in a separate thread.
#  - webview.load_url(url)
# 
# Load a new URL into a previously created WebView window. This function must be invoked after WebView windows is created with create_window(). Otherwise an exception is thrown.

# In[ ]:


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
            self.url_list = d[d['hiv'].isnull()]['domain'][:limit].tolist()
            self.past_df = d[d['hiv'].notnull()]

        except :
            # start from 0
            d = pd.read_csv('domains.csv')
            self.url_list = d['domain'][:limit].tolist()
            self.past_df = None



        self.lbls = ['hiv','health','gov','edu','ngo']
        # init dataframe
        self.data_lbl = d
        for lbl in self.lbls :
            self.data_lbl[lbl] = None
        
    def run(self):
        idx=0
        for url in self.url_list :
            
            print "Starting " + self.name + " on " + url

            webview.load_url(url)
            
            print 'Getting input...'
            for lbl in self.lbls :
                while True : #ask for input while the user makes mistakes
                    try :
                        rep = input(lbl + '?')
                        self.data_lbl.loc[idx,lbl] = rep
                    except :
                        print "Please enter 0 or 1"
                    else :
                        break
            idx+=1
            if self.past_df is None:
                self.data_lbl.to_csv('dom_lbl.csv', index=False)
            else :
                pd.concat([self.past_df,self.data_lbl])                   .to_csv('dom_lbl.csv', index=False)
            

# Create new threads
web_thread = webThread(1, "Web Thread", 2)

# Start new Threads
web_thread.start()

webview.create_window('Please label this site', width=1200, height=800, resizable=True, fullscreen=False)


# In[ ]:


import pandas as pd
# fetch most recent version
d = pd.read_csv('dom_lbl.csv')



# In[ ]:


d[d['hiv'].notnull()]

