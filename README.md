# Report Loader Application

# Table of contents

  - [Reporter API](#reporter-api)
  - [Overview](#overview)
  - [Table of Contents](#table-of-contents)
  - [Development with Docker](#Development-with-Docker)
  - [Running Application](#running-application)
  - [System Architecture](#system-architecture)
  - [License](#license)
  
## Overview
RLA (Report Loader Application) is the PA service that encapsulates the functionality 
of pulling the reporting data from SSP platforms and pushing them to PA UI database. 
The basic functions are the following:
1. Pull Data From Platform
2. Push Data To UI
The entry point of the application is a JsonRPCServer that can serve the pull_data and
push_data commands. The response of these commands is a report that describes the 
results of these actions.


## Development with Docker
The development of the application is done by using the docker and docker-compose tools.
By following the steps below you can have a development environment for the RLA
application:
1. git clone http://damianos@gitlab/damianos/reports_loader.git
2. cd reportloader
3. docker-compose build
4. docker-compose up
5. docker-compose exec <container-name> bash (SSH to a running container)
6. docker-compose down (Stop the application)

## Test the application
In order to test the application run the following commands inside **web** container:
```sh
python3 reporterapi/tests/pull_data.py
python3 reporterapi/tests/data_pusher.py
```
## How to use the Reporter API
There are three (3) ways to access the function of the RLA service
1. By using the web Interface of the Reporter API
[Reporter API web interface](http://reporterapi.projectagora.net/v1.0/ui/#)
  1. Use the "Call the service" command to post the action
  2. Use the "Get the service Data" command to get the results
2. By using python code scripting

```python
headers = 
{
  'Content-Type': 'application/json',
  'X-Api-Key': 'MTQ0NjJkZmQ5OTM2NDE1ZTZjNGZmZjI3'
}

 ### post the service to be run
 request_body = {
        'host':'tw-reporter.jenkins',
        'port':'5552',
        'method':'pull_data',
        'retry': False,
        'retry_delay':0,
        'blocked':True,
        'params' :{'platform':'adx', 'startdate': '2018-10-01',
                   'enddate': '2018-10-01'}
 }
        
 try:
    response = requests.post(url = '{0}://{1}:{2}/v1.0/service'
                             .format('http', 'reporterapi.jenkins', '5010'), 
                              data = json.dumps(request_body), 
                              headers = self.headers)
 except Exception as e:
    print('Error in post service request')  

 task_id = response.json()['id']

 try:
    while True:
       time.sleep(3)        
        response = requests.get(url = '{0}://{1}:{2}/v1.0/service/{3}'
                         .format('http', 'reporterapi.jenkins', '5010', task_id), 
                        headers = self.headers)
    
	    if response.json()['status'] == 'PENDING':
	        continue
	    elif response.json()['status'] == 'RETRY':
	        data = 'RETRY'
	    elif response.json()['status'] == 'FAILURE':
	        data =  False
	    else:
	        data = response.json()['data']['result']
        
 except Exception as e:    
     print('Error in get service result request')
``` 

3. Use the agora-cli tool to run the pull_data and push_data commands.

## System Architecture
The architecture of the Reporter API is depicted in the following figure:


License
-------

Copyright 2017 Tailwind Software

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at [apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
