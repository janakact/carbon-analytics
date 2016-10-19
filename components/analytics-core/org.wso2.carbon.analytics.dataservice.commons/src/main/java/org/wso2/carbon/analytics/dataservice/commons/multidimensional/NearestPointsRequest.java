/*
 *  Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.carbon.analytics.dataservice.commons.multidimensional;


public class NearestPointsRequest extends MultiDimensionalRequest {
    private double latitude;
    private double longitude;
    private long numberOfPoints;

    public NearestPointsRequest(String tableName, String columnName, double latitude, double longitude, long numberOfPoints)
    {
        this.tableName = tableName;
        this.columnName = columnName;
        this.latitude = latitude;
        this.longitude = longitude;
        this.numberOfPoints = numberOfPoints;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public long getNumberOfPoints() {
        return numberOfPoints;
    }
}
