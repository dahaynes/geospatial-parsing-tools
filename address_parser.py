import csv, datetime
from collections import OrderedDict, Counter
from copy import deepcopy


class processing(object):

    def __init__(self, inShapeFilePath, outCSV, primaryKey="FID"):

        import arcpy, math, re
    
        self.arcpy = arcpy
        self.re = re
        self.baseDir = baseDir = '\\'.join(inShapeFilePath.split('\\')[:-1])
        self.shapeFilePath = inShapeFilePath
        self.anomalies = {"lot":r"lot", "hashtag": r"#", "apt":"apt","box":"box"}

        self.WriteShapeFileToCSV(self.shapeFilePath, outCSV)
        self.geomDict = self.CreateGeometryDict(self.shapeFilePath)
        

        #error catching for the class
        try:
            self.data = self.ReadCSV(outCSV,primaryKey)
        except:
            print("Please provide a new Primary key, not %s" % (primaryKey)) 

        if self.data:
            self.GeocodingResults()
            self.unmatchedData = self.FilterDataset(self.data, 'Loc_name', ' ')
            

                
            #self.ParseAnomalies()
            
        
    def ReadCSV(self, theCSVPath, thePrimaryKey="FID"):
        """
        This function reads in the data csv and keeps it in its original format using OrderedDict
        Specify the primaryKey = "ObjectID"
        The nesting structure is annoying but useful

        Create a list of tuples for each field [ (filedName, fieldValue) ....]
        This preserves the order of the attributes, mostly important when writing the data out
        OrderedDict( [(field, line[field]) for field in fieldnames] )

        These are packed together into another OrderedDictionary perserving the row order
        OrderedDict( (int(line[thePrimaryKey]),  OrderedDict( [(field, line[field])
        """
            
        with open(theCSVPath, "r") as inFile:
            theReader = csv.DictReader(inFile, delimiter=",")
            fieldnames = theReader.fieldnames
            outDataset = OrderedDict( (int(line[thePrimaryKey]),  OrderedDict( [(field, line[field]) for field in fieldnames] )  ) for line in theReader )

            return outDataset
    
    def WriteFile(self, filePath, theDictionary):
        """
        This function writes out the dictionary as csv
        """
        
        thekeys = list(theDictionary.keys())
        
        with open(filePath, 'wb') as csvFile:
            fields = list(theDictionary[thekeys[0]].keys())
            theWriter = csv.DictWriter(csvFile, fieldnames=fields)
            theWriter.writeheader()

            for k in theDictionary.keys():
                theWriter.writerow(theDictionary[k])

    def CreateGeometryDict(self, aFeatureClass):
        """
        This method will creat a geometry dictionary of the x,y location that can be joined 
        """
        myGeometryFields = []
        fieldNames = [thefield.name for thefield in self.arcpy.ListFields(aFeatureClass)]

        if "Shape" in fieldNames: myGeometryFields.append("Shape")
        if "FID" in fieldNames: myGeometryFields.append("FID")

        if len(myGeometryFields) == 2:
            geomDict = {}
            with self.arcpy.da.SearchCursor(aFeatureClass, myGeometryFields) as cursor:
                geomDict = {r[1]: OrderedDict([ ('x', r[0][0]), ('y', r[0][1]) ]) for r in cursor}
                    
               
        return geomDict
            
            

    def WriteShapeFileToCSV(self, aFeatureClass, outCSV):
        """
        This function writes the shapefile/FeatureClass to the CSV provided.
        Ignored some awful fields.
        """
        fieldNames = [thefield.name for thefield in self.arcpy.ListFields(aFeatureClass)]

        if "Shape" in fieldNames: fieldNames.remove("Shape")
        if "GEOG" in fieldNames: fieldNames.remove("GEOG")

        #print(fieldsNames)
        with self.arcpy.da.SearchCursor(aFeatureClass, fieldNames) as cursor, open(outCSV, 'wb') as outFile:
            theWriter = csv.writer(outFile, delimiter=",")
            theWriter.writerow(fieldNames)
            for rec in cursor:
                theWriter.writerow(rec)
        
        

    def GeocodingResults(self,):
        """
        This function reports the number of items that have been geocoded
        """
        
        allLocators = [self.data[k]['Loc_name'] for k in self.data.keys()]
        cnt = Counter()
        for locator in allLocators:
            cnt[locator] += 1

        for item in cnt:
            print("Locator: %s matched %s cases, %s"  % (item, cnt[item], cnt[item]/float(len(self.data))*100 ))

    def OutputMatched(self, outFilePath):
        """
        This is a meta function that will allows for the output of the already matched geocoded data.
        """

        unmatchedKeys = self.unmatchedData.keys()
        self.matchedData = {rec:self.data[rec] for rec in self.data.keys() if rec not in unmatchedKeys}


        #This is terrible, but I wanted to reduce the number of unnecessary fields that are added to the database
        #not that the database cares. But it seems like you want to reduce the number of fields in the create statement.
        self.geocodeTable = {rec: OrderedDict([
                                   ('FID', self.matchedData[rec]['FID']),
                                   ('LocatorName', self.matchedData[rec]['Loc_name']),
                                   ('Score', self.matchedData[rec]['Score']),
                                   ('Address', self.matchedData[rec]['Match_addr']),
                                   ('Stand_add', self.matchedData[rec]['ARC_Street']),
                                   ('city', self.matchedData[rec]['ARC_City']),
                                   ('state', self.matchedData[rec]['ARC_State']),
                                   ('zip', self.matchedData[rec]['ARC_ZIP']),
                                   ('address_id', self.matchedData[rec]['address_id']),
                                   ('x', self.geomDict[rec]['x']),
                                   ('y', self.geomDict[rec]['y']),
                                   ]) for rec in self.matchedData.keys() }
        
        self.WriteFile(outFilePath, self.geocodeTable)
            
    def ParseAnomalies(self):
        """
        Metafuction running the CleanOddity task and writing out the anomaly subset files
        """
        for a in self.anomalies.keys():
            oddDataSet = self.IdentifyAnomaly( 'street_add', self.anomalies[a])
            anomalyFilePath = r"%s\geocode_%s_%s.csv" % (self.baseDir, a, str(datetime.date.today()).replace("-","_") )
            if oddDataSet:
                self.WriteFile(anomalyFilePath, oddDataSet)
            else:
                print("Not writing out file %s, no records" % (anomalyFilePath) )

    def IdentifyAnomaly(self, theKey, theAnomaly):
        """
        This function is used the ParseAnomalies to segment the data.
        """
        
        oddData = {i:self.data[i] for i in self.data.keys() if theAnomaly in self.data[i][theKey].lower()}
        
        print( "%s Group: %s has %s items %s"   % ("*"*10, theAnomaly, len(oddData), "*"*10)  )
        for i in oddData.keys():
            address = oddData[i][theKey].lower()
            match = self.re.search(theAnomaly, address)
            #print(i, oddData[i][theKey], oddData[i][theKey][:match.start()], match.start())
            oldKey = "%s_old" % (theKey)
            oddData[i][oldKey] = oddData[i][theKey]
            if match.start():
                oddData[i][theKey] = oddData[i][theKey][:match.start()]
            else:
                oddData[i][theKey].replace(theAnomaly, "")

        return oddData


    def FilterDataset(self, dataDictionary, key, value):
        """
        """

        filteredDataset = {d: dataDictionary[d] for d in dataDictionary.keys() if dataDictionary[d][key] == value}

        return filteredDataset


    
    def ProcessPOBOX(self, unmatchedRecords, keyField='street_add', anomaly='box'):
        """
        This is the sample that i based by analytics from
        testdata = ['P O Box 337  216 West Front Street #11', '15355 Doc Rd Hwy 89 P.O. Box 717', 'Rte 1 Box 147']
        
        """
        
        for rec in unmatchedRecords.keys():
            #print("Matching %s" % (rec[keyField]) )
            
            #make every element in the address lower case
            addressElements = [x.lower() for x in unmatchedRecords[rec][keyField].split(" ") ]

            #Match the anomaly in the address
            try:
                anomalyIndex = addressElements.index(anomaly)

                #This removes the next occuring element if it is alpha or numeric
                # 106B
                if addressElements[anomalyIndex+1].isalnum():
                    addressElements.remove(addressElements[anomalyIndex+1])
                
                #This works backwards from where the anomaly is found, removing items that match either p | p. | o | o.
                for i in reversed(addressElements[:anomalyIndex]):
                    if i in ['p', 'p.', 'o' , 'o.', 'po', 'p.o.']:
                        #['rr', 'r', 'r.r.' 'rr', 'route', 'rte' 'r r']
                        #print("Matched %s" % (i))
                        addressElements.remove(i)
                    else:
                        #using this like a while loop to stop removing elements after a certain point
                        break

                addressElements.remove(anomaly)
                newAddress = " ".join(addressElements).strip()
                #print("New Address: %s " % (newAddress))
                unmatchedRecords[rec]['fixedAddress'] = newAddress
                #fixedAddress.append(newAddress)

            except ValueError:
                #Ignoring records that don't match
                pass
            except IndexError:
                #There is nothing after anomaly is detected and this is at the end of the list
                pass

        fixedRecords = {unmatchedRecords[k]["FID"]:unmatchedRecords[k] for k in unmatchedRecords.keys() if 'fixedAddress' in unmatchedRecords[k].keys()}
        unfixedRecords = {unmatchedRecords[k]["FID"]:k for k in unmatchedRecords.keys() if 'fixedAddress' not in unmatchedRecords[k].keys()}


        return fixedRecords, unfixedRecords
            
    def Process(self, ):
        pass

        

    
 

