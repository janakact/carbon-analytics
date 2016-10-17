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

import org.wso2.carbon.analytics.dataservice.commons.exception.AnalyticsNotExicutedException;

/**
 * Created by wso2123 on 10/14/16.
 */
public class RangeBucket extends MultiDimensionalBucket {
    private Number[] lowerBound;
    private Number[] upperBound;


    public RangeBucket(String lable, Number[] lowerBound, Number[] upperBound)
    {
        this.label = lable;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public long getCount() throws AnalyticsNotExicutedException {
        if(count!=-1)
            return count;
        else
            throw new AnalyticsNotExicutedException("Exicute Set Query");
    }
}
