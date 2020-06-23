# everything_analysis

###API
```TOKEN=$(curl -s https://babeshop.ftech.ai/api/auth -d '{"username": "me", "password": "w4J6OObi996nDGcQ4mlYNK4F"}' | jq -r .access_token)```

```curl -H "Authorization: Bearer $TOKEN" -s https://babeshop.ftech.ai/api/conversations > conv.json```

```curl -H "Authorization: Bearer $TOKEN" -s https://babeshop.ftech.ai/api/conversations/3249860898460524 | less```

```"sender_id": 3249860898460524```
##Purpose 
Develop core metrics to track performance of salesbot. 
##Approach/ Propositions 
Proposed metric: 
(Level 1) Success Rate: is defined as below 

Number of successful conversationsAll conversations


Successful conversations: successful conversations are marked by thanks or order/ shipping requests at the end of the conversations. 

To count number of successful conversations, we count: 
how many chatlogs (conversations) end up with thanks 
how many chatlogs (conversations) end up with order request 
and then add them up together 

[Describe how you count "thanks" chatlogs]

..............



[Describe how you count "order/ shipping" chatlogs]

...............
##Data Description
[You may have to briefly describe the data (chatlog) you analyze . How many chatlog in total, some descriptive statistics might be interesting such as the average number of turns of each conversations, since when bot goes live, etc.]
##Results 

We report success rate over days/ weeks/ months since bot went live, and report results across versions. 
[Insert results, provide graphs & charts, ...] 

##Discussions
