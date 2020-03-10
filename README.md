# obps_metrics
The obps_metrics is a set of python scripts developed at the IOC Ocean Best Practices System (IOC-OBPS) https://www.oceanbestpractices.org/ in order to gather metrics about the own OBPS performance that are dispalyed in a set of html web front-ends through the obps_dashboard project https://github.com/cmunozmas/obps_dashboard.

## Prerequisites:

    See requirements file for extralibraries needed.

## How to install and run obps_metrics:
- Create a virtual environment and install the requirements.
- Import the database structure contained within the obps_metrics_template.sql file in /config directory.
- Configure the credentials for google analytics and database in Config.py file contained in /config project directory.
- Configure paths of the bash scripts contained in /scripts directory.
- Copy client sectrets oceanbestpractices-195713-d1c8069837c8.json file into /json directory.
- set up a cron job as follows:

      SHELL=/bin/bash
      00 00   * * *   root    PYTHONPATH=/Path/to/.virtualenvs/myvirtualenv/bin/python /Path/to/obps_metrics/scripts/last30days_analytics.sh > /Path/to/obps_metrics/log/e$
      00 10   1 * *   root    PYTHONPATH=/Path/to/.virtualenvs/myvirtualenv/bin/python /Path/to/obps_metrics/scripts/historic_analytics.sh > /Path/to/obps_metrics/log/err$

- To gather monthly metrics since December 2017, edit the historic_analytics.py by uncommentin lines 17 and 18:
    
      date_start = '2017-12-01'
      date_end = '2020-03-01'


    

## The following features are planned or in development:

  

## Copyright

Copyright (C) 2020 International Oceanographic Data and Information Exchange" (IODE) of the "Intergovernmental Oceanographic Commission" (IOC) of UNESCO http://www.iode.org

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program. If not, see http://www.gnu.org/licenses/.
