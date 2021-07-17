# READ Me ~ Analysis of the rivers in the Bolzano'S area   


Add a Table of Contents (Optional) ...
How to Install Your Project. ...
How to Use Your Project. ...
Include Credits. ...
List the License. ...
Badges

# 1
## DESCRIPTION OF THE PROJECT  
The aim of this project is to create a robust and persistent pipeline that allows to handle and work with big data relatively to the state of the rivers in the Bolzano's Area. 
More precesily, what we want to achieve is to collect, store and analyse data regarding the Water Level, the Water Temperature and the Water flow of three different rivers called Adige, Talvera and Isarco. 
In short, we collected both historical data (from the following link: http://meteobrowser.eurac.edu/app_direct/meteobrowser/?_inputs_&csvjson=%22json%22&daterange=%5B%222019-01-01%22%2C%222021-07-18%22%5D&gather=%22wide%22&gather-selectized=%22%22&isdst=true&language=%22en%22&map_bounds=%7B%22north%22%3A46.531469060889%2C%22east%22%3A11.4419174194336%2C%22south%22%3A46.4369104957813%2C%22west%22%3A11.2112045288086%7D&map_center=%7B%22lng%22%3A11.32648%2C%22lat%22%3A46.4842759763911%7D&map_groups=%5B%22Street%20Map%22%2C%22draw%22%5D&map_marker_mouseout=%7B%22id%22%3Anull%2C%22.nonce%22%3A0.0469041332985507%2C%22lat%22%3A46.4423%2C%22lng%22%3A11.2525%7D&map_marker_mouseover=%7B%22id%22%3Anull%2C%22.nonce%22%3A0.127517571308739%2C%22lat%22%3A46.4423%2C%22lng%22%3A11.2525%7D&map_zoom=12&refresh=8&round=%22hourly%22&round-selectized=%22%22&selAltitude=%5B0%2C3399%5D&selSensor=%5B%22Water%20flow%20-%20m%C2%B3%2Fs%22%2C%22Water%20level%20-%20cm%22%2C%22Water%20temperature%20-%20%C2%B0C%22%5D&selSensor-selectized=%22%22&selStation=%5B%22EISACK%20BEI%20BOZEN%20S%C3%9CD%20%2F%20ISARCO%20A%20BOLZANO%20SUD%22%2C%22ETSCH%20BEI%20SIGMUNDSKRON%20%2F%20ADIGE%20A%20PONTE%20ADIGE%22%2C%22TALFER%20BEI%20BOZEN%20%2F%20TALVERA%20A%20BOLZANO%22%5D&selStation-selectized=%22%22&sidebarCollapsed=false&sidebarItemExpanded=null) and new data (collected in real time through the API at the following link:http://dati.retecivica.bz.it/services/meteo/v1/sensors) regarding the above mentioned measures for each of the above mentioned rivers. 
We stored these information into a relational data base, namely a MySQL db. Then, we used this data to train a SARIMAX model, an autoregressive model, that allows us to make predictions up to a week after the last observation stored. 
The output of this model is a WebApp that shows the rivers in an interactive map. Then the user can select the rivers and the measure he/she is interested in. Then, it is possible to see a plot of that specific measures.  Lastly, thanks to a slider from 1 to 168 it is possible to select a number that corresponds to the predicion time. For example, if you choose 1 you will make a prediction of the state of the rivers for the next hour, the furthest prediction is obtained with 168 namely, a week after the last observation. This, will produce a plot where it is possible to visualize the latest trend of the selected variable for the choosen river plus the preediction.  The prediction is represented as a solid gray line that shows the range of values in which the prediction can lie in the choosen interval. 

# 2 
## HOW TO RUN THE CODE
### Before starting... we need to be connected...
It is important to recall that instead of installing all the applications needed, the authors decided to create an _EC2_ instance in _AWS_. Then after having activated the instance, we dowloaded docker in this virtual machine. Then thanks to docker, we were able to use an image of MySQL without the need to download it. Thus, to activate the MySQL service we need to:
A) open _PuTTY_, a free and open-source terminal emulator; 
B) use the instnace name as the host name for _PuTTY_; 
C) specify the private key file for autentication, which has been dowloaded from  AWS;
D) log in as `ec2-user`, accessing the right directory : `bdt_dir`; 
E) then through the `docker-compose.yml` we finally activate the service and we establish the connection to the db.   

_PuTTY_ has to be on and connected  in order to perform all the following computations

### Load & Store historic data 
The first step of our project is to collect the historical data from _Meteo Browser SudTirol_. 
Thus, we dowload as a json file the information of the rivers from the 1st January 2019 until today. We save this file as `historic_data.json`. 
Then, to store this data into our db,  we need to open two different terminals: 
1) In the first one, we have to launch `mqtt.py` the file where we have specified all the instruction for our pub-sub engine. 
29 In the second one, we have to launch `pyspark_dati_storici.py` which transform and publish the data as csv that will be stored into a relational db. 
More precisely, to each river is associated a table e.g. `tabella_talvera`. In addition, we have create a table where are stored the names of the rivers and associated index. 
This choice has been made in order to avoid to collect unseful and repetitive information as the name of the rivers for each observation. 
### Load & Store new data 
This task can be accomplished in two differnt ways: thanks to `rivers_operation.py` or `pyspark_dati_nuovi.py`. However, this last method is slower because the new data are just three observation hence, it is not possible to take the advantages connected to Spark technology. 
Nevertheless, the processes to load and store the new data does not change and so can be adapt to each situation.
The procedure is the same for the historic data: 
1)  we have to launch `mqtt.py` 
2)  we have to call  `rivers_operation.py` or `pyspark_dati_nuovi.py`.
### Model and predictions 
When we run `rivers_operation.py` we are also saying to the machine to make predictions on the state of the rivers after 1hour, 3hours, 12hours, one day, three days and one week. 
These observations are then stored in a new tabel where we collect the `Timestamp` of the last observation, and the prediction for each time horizon. 
The predictions are made thanks to the autoregressive model implemented in `analysis_FINAL.py`. 
### Final Output: The WebApp
Lastly, to visualize the output of the model we have to run the file `WebApp_FINAL.py`. This will create the WebApp but, to visualize it on the browser we need to run the following code in the terminal `streamlit run WebApp_FINAL.py` or to run on the browser the http that have been output after having run the `WebApp_FINAL.py`.
