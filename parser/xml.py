import os
import re
from uuid import uuid4
from pandas import DataFrame, concat, options
from re import findall
from parser.functions import strip_namespace, convert_datetime
import xml.etree.ElementTree as ET
import datetime
# import logging
import warnings
import logging
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd

options.mode.chained_assignment = None
logging.basicConfig(filename='C:/Users/Corey/OneDrive/OneDrive - New Mexico Highlands University/Python/FFI/XMLToCustom/log/data.log',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M')


class FFIFile:
    """
    this is a class that represents the entire XML file. It can be thought of as a collection of 'tables' represented by
    the element names that appear in the XML file.
    """

    def __init__(self, file):
        """
        parses a ElementTree root element and creates the FFIFile class
        """
        # with open(file) as open_file:
        #     f_gen = (open_file.readline() for i in range(50000))
        #     f = '\n'.join(f_gen)
        #     file_hash = sha256(f.encode())
        #     file_id = file_hash.hexdigest()

        # self._id = file_id
        self.file = file.name.strip('.xml')
        self._tree = ET.parse(file)
        self._root = self._tree.getroot()
        self._namespace = findall(r'\{http://\w+\.\w{3}[\w/.\d]+\}', self._root.tag)[0].strip('{}')
        self._base_tables = {}
        self._data_map = {}
        self._excluded = ['FuelConstants_DL', 'FuelConstants_ExpDL', 'FuelConstants_FWD', 'FuelConstants_Veg',
                          'FuelConstants_CWD', 'Schema_Version', 'Program', 'Project', 'DataGridViewSettings',
                          'MasterSpecies_LastModified', 'Settings']
        self._processed = []
        # self._tables = {}
        # self._filtered = False
        self._retry_tables = {}
        self.duplicate = False
        self.dup_on = None
        self.many_tables = False
        self._user = os.environ['USERNAME']

        print(f"Reading data for {self.file}")
        logging.info(f"Transforming tables for {self.file}")
        self._parse_data()
        self._parse_idents()
        self.version = self['Schema_Version']['Schema_Version'][0]
        self.admin_unit = self['RegistrationUnit']['RegistrationUnit_Name'][0]
        self.insert_failed = []
        # current = datetime.datetime.now()
        # self.log_file = f"Migration_Log_{current.year}{current.month}{current.day}{current.hour}{current.minute}{current.second}.log"
        # self._parse_idents()

    def __setitem__(self, key, value):
        if type(value) == pd.DataFrame:
            self._data_map[key] = value
        else:
            raise TypeError(f"Please provide a DataFrame object, not a {type(value)}.")

    def __getitem__(self, item):
        """
        I needed to create some way to index the FFIFile class, so this will pass the index to the data_map and return
        whatever that operation returns.

        e.g <FFIFile>['column'] returns <internal DataFrame>['column']
        """

        if item in self._data_map.keys():
            return self._data_map[item]
        else:
            raise KeyError(f'{item} not in FFI XML file.')

    @staticmethod
    def _update_last_modified(self, session):
        """
        Just updates the LastModified table with current user
        """
        # gather computer name and username as well as current time
        comp_name = os.environ['COMPUTERNAME']
        user = os.environ['USERNAME']
        now = str(datetime.datetime.now())

        # use a dict of this info to create a DataFrame
        lm_dict = {'last_edit_date': [now],
                   'Machine_Name': [comp_name],
                   'User_Name': [f'{comp_name}\\{user}']}
        last_modified = DataFrame(lm_dict)

        # overwrite last modified
        last_modified.to_sql('Last_Modified_Date', session.bind, index=False, if_exists='replace')

    def _parse_data(self):
        """
        Iterates through each element name that was produced in the __init__ operation. This is what actually populates
        the data_map element
        """
        # needed_tables = ['MacroPlot', 'RegistrationUnit', 'MM_ProjectUnit_MacroPlot', 'ProjectUnit', 'SampleEvent',
        #                  'MM_MonitoringStatus_SampleEvent', 'MonitoringStatus', 'MethodAttribute', 'AttributeData',
        #                  'Method', 'LU_DataType', 'Schema_Version', 'MasterSpecies', 'SampleData', 'SampleAttribute',
        #                  'LocalSpecies']

        tags = set([strip_namespace(element.tag) for element in self._root])
        for tag in tags:
            all_data = self._root.findall(tag, namespaces={'': self._namespace})
            dfs = [
                DataFrame({strip_namespace(attr.tag): [attr.text] for attr in data_set})
                for data_set in all_data
            ]
            df = concat(dfs)
            for col in df.columns:
                if '_GUID' in col:
                    df[col] = df[col].apply(lambda row: row.upper())
                elif 'Date' in col or 'Time' in col:
                    df[col] = df[col].apply(lambda row: convert_datetime(row))
            self._data_map[strip_namespace(tag)] = df.reset_index(drop=True)

    def _parse_idents(self):
        """
        This generates PlotID and EventID columns that are used in insertions into the Access database.
        Logic is in Python as it's way easier to handle than SQL.

        Calling will result in the MacroPlot table to have a new column called 'PlotID', and  the SampleEvent table
        to have a new column called 'EventID'.

        This will allow easy lookup when converting from the GUID to the generated ID when methods (and everything else)
        get inserted into the tables, and we need the generated ID for key matching.
        """

        def create_id(row, id_type):
            """
            These functions within functions are annoying, but easier than having it at a higher scope.

            This is just a helper function in generating appropriate IDs for plots and events to match with Access

            For use only within an .apply(), where the row is guaranteed to have the defined columns. Otherwise, this
            will all break. You can also easily add logic for additional identifiers that need to be created fairly
            easily by adding additional elif statements to both the create_id function and the outer logic.
            """

            id_value = ""
            if id_type == 'plot':
                admin_guid = row['MacroPlot_RegistrationUnit_GUID']
                plot_guid = row['MacroPlot_GUID']
                plot_name = row['MacroPlot_Name']

                admin_unit = self['RegistrationUnit'].loc[
                    self['RegistrationUnit']['RegistrationUnit_GUID'] == admin_guid
                ]['RegistrationUnit_Name'].values[0]
                admin_unit = admin_unit.replace(" ", "").replace("_", "").replace("-", "").replace(".", "").upper()
                plot_name = plot_name.replace(" ", "").replace("_", "").replace("-", "").replace(".", "").upper()
                id_value = admin_unit[:5] + plot_name

            elif id_type == 'event':
                plot_guid = row['SampleEvent_Plot_GUID']
                try:
                    plot_id = self['MacroPlot'].loc[
                        self['MacroPlot']['MacroPlot_GUID'] == plot_guid
                    ]['PlotID'].values[0]
                except IndexError:
                    plot_id = ''

                if plot_id != '':
                    date_re = re.findall(r'(\d{4}-\d{2}-\d{2})', row['SampleEvent_Date'])
                    date_raw = date_re[0]
                    event_date = date_raw.replace('-', '')
                    id_value = plot_id + event_date
                else:
                    id_value = ''

            return id_value
        # end of function

        for table in ['MacroPlot', 'SampleEvent']:
            temp_df = self[table]

            if table == 'MacroPlot':
                temp_df['PlotID'] = temp_df.apply(create_id, axis=1, args=('plot',))
                temp_df.dropna(subset='MacroPlot_DateIn', inplace=True)
                temp_df = temp_df.sort_values('MacroPlot_DateIn').drop_duplicates('PlotID', keep='first')
            elif table == 'SampleEvent':
                temp_df['EventID'] = temp_df.apply(create_id, axis=1, args=('event',))
                temp_df.dropna(subset='EventID', inplace=True)
                temp_df = temp_df[temp_df['EventID'] != '']
                temp_df = temp_df.sort_values('EventID').drop_duplicates('EventID', keep='first')

            self[table] = temp_df

    def _attr_to_many(self):
        """
        Converts the AttributeData and AttributeRow tables into the many-tables format used by FFIMT
        """

        # These first few blocks are self-explanatory
        select_list = ['EventID', 'SampleData_SampleEvent_GUID', 'AttributeRow_DataRow_GUID',
                       'MethodAtt_FieldName', 'AttributeData_Value', 'Method_Name', 'Method_UnitSystem']

        select_rename = {'AttributeRow_DataRow_GUID': 'AttributeData_DataRow_GUID',
                         'SampleRow_Original_GUID': 'AttributeData_SampleRow_GUID',
                         'AttributeRow_Original_GUID': 'AttributeData_Original_GUID',
                         'AttributeRow_CreatedBy': 'AttributeData_CreatedBy',
                         'AttributeRow_CreatedDate': 'AttributeData_CreatedDate',
                         'AttributeRow_ModifiedBy': 'AttributeData_ModifiedBy',
                         'AttributeRow_ModifiedDate': 'AttributeData_ModifiedDate'}

        attr_data = self['AttributeRow'] \
            .merge(self['AttributeData'],
                   left_on='AttributeRow_ID',
                   right_on='AttributeData_DataRow_ID', how='left') \
            .merge(self['MethodAttribute'],
                   left_on='AttributeData_MethodAtt_ID',
                   right_on='MethodAtt_ID', how='left') \
            .merge(self['Method'],
                   left_on='MethodAtt_Method_GUID',
                   right_on='Method_GUID', how='left') \
            .merge(self['SampleRow'],
                   left_on='AttributeData_SampleRow_ID',
                   right_on='SampleRow_ID', how='left') \
            .merge(self['SampleData'],
                   left_on='AttributeData_SampleRow_ID',
                   right_on='SampleData_SampleRow_ID', how='left') \
            .merge(self['SampleEvent'],
                   left_on='SampleData_SampleEvent_GUID',
                   right_on='SampleEvent_GUID', how='left')
        try:
            attr_select = attr_data[select_list]
        except KeyError:  # these fields are in the SQL tables, but aren't included in the XML
            # I can probably get rid of this, but I'm not sure how the indexing and renaming would work, so I'll
            attr_data['AttributeRow_CreatedBy'] = pd.NA
            attr_data['AttributeRow_CreatedDate'] = pd.NA
            attr_data['AttributeRow_ModifiedBy'] = pd.NA
            attr_data['AttributeRow_ModifiedDate'] = pd.NA
            attr_select = attr_data[select_list]

        attr_long = attr_select.rename(columns=select_rename)  # renaming columns
        methods = attr_long['Method_Name'].unique()
        for method in methods:
            print(method)
            temp = attr_long.loc[attr_long['Method_Name'] == method].drop_duplicates()
            subset = temp.pivot(index=['EventID', 'SampleData_SampleEvent_GUID',
                                       'AttributeData_DataRow_GUID', 'Method_UnitSystem'],
                                columns=['MethodAtt_FieldName'],
                                values='AttributeData_Value').reset_index()
            unit_systems = subset['Method_UnitSystem'].unique()
            table_name = method.replace(' ', '').replace('-', '_').replace('(', '_').replace(')', '_').strip('_')

            for col in subset.columns:
                if 'Spp' in col:
                    spp_df = self['LocalSpecies']
                    subset['Species'] = subset.apply(lambda row:
                                                     spp_df.loc[
                                                         spp_df['LocalSpecies_GUID'] == row[col].upper()
                                                     ].iloc[0]['LocalSpecies_Symbol'],
                                                     axis=1)

            if method == 'Trees - Individuals':
                subset['StemNum'] = subset.groupby(['EventID', 'Species', 'TagNo']).cumcount() + 1
            elif method == 'Plot Info Wit Trees Comments3':
                if 'WitTreeTagNo' not in subset.columns:
                    subset['WitTreeTagNo'] = subset.groupby(['EventID']).cumcount() + 1
                subset.sort_values(['EventID', 'WitDBH'], inplace=True)
                subset.drop_duplicates('EventID', keep='first', inplace=True)
                # subset['StemNum'] = subset.groupby(['EventID', 'WitTreeTagNo']).cumcount() + 1
            # elif method == 'Trees - Seedlings (Height Class)':
            #     subset['Status'] = subset['Status'].fillna('Unk')
            subset.dropna(subset=['EventID'], inplace=True)

            if len(unit_systems) > 1:
                for unit_system in unit_systems:
                    unit_subset = subset.loc[subset['Method_UnitSystem'] == unit_system]
                    if unit_system != 'English':
                        sql_table = f"{table_name}_{unit_system}_Attribute"
                    else:
                        sql_table = f"{table_name}_Attribute"
                    self._data_map[sql_table] = unit_subset
            else:
                sql_table = f"{table_name}_Attribute"
                subset.drop(columns=['Method_UnitSystem'], axis=1, inplace=True)
                self._data_map[sql_table] = subset

    def _sample_to_many(self):
        select_list = ['SampleRow_Original_GUID', 'SampleData_SampleEvent_GUID', 'SampleAtt_FieldName',
                       'SampleData_Value', 'SampleRow_CreatedBy', 'SampleRow_CreatedDate', 'SampleRow_ModifiedBy',
                       'SampleRow_ModifiedDate', 'Method_Name', 'Method_UnitSystem']
        select_rename = {'SampleRow_Original_GUID': 'SampleData_SampleRow_GUID',
                         'SampleRow_CreatedBy': 'SampleData_CreatedBy',
                         'SampleRow_CreatedDate': 'SampleData_CreatedDate',
                         'SampleRow_ModifiedBy': 'SampleData_ModifiedBy',
                         'SampleRow_ModifiedDate': 'SampleData_ModifiedDate'}

        sample_data = self['SampleRow'] \
            .merge(self['SampleData'],
                   left_on='SampleRow_ID',
                   right_on='SampleData_SampleRow_ID', how='left') \
            .merge(self['SampleAttribute'],
                   left_on='SampleData_SampleAtt_ID',
                   right_on='SampleAtt_ID', how='left')\
            .merge(self['Method'],
                   left_on='SampleAtt_Method_GUID',
                   right_on='Method_GUID', how='left')
        try:
            sample_select = sample_data[select_list]
        except KeyError:
            sample_data['SampleRow_CreatedBy'] = pd.NA
            sample_data['SampleRow_CreatedDate'] = pd.NA
            sample_data['SampleRow_ModifiedBy'] = pd.NA
            sample_data['SampleRow_ModifiedDate'] = pd.NA
            sample_select = sample_data[select_list]

        sample_long = sample_select.rename(columns=select_rename)
        sample_long['SampleData_Original_GUID'] = sample_long.apply(lambda _: str(uuid4()).upper())
        methods = sample_long['Method_Name'].unique()
        for method in methods:
            temp = sample_long.loc[sample_long['Method_Name'] == method]
            subset = temp.pivot(index=['SampleData_SampleRow_GUID', 'SampleData_SampleEvent_GUID',
                                       'SampleData_Original_GUID', 'SampleData_CreatedBy',
                                       'SampleData_CreatedDate', 'SampleData_ModifiedBy',
                                       'SampleData_ModifiedDate', 'Method_UnitSystem'],
                                columns=['SampleAtt_FieldName'],
                                values='SampleData_Value').reset_index()
            unit_systems = subset['Method_UnitSystem'].unique()
            table_name = method.replace(' ', '').replace('-', '_').replace('(', '_').replace(')', '_').strip('_')
            if len(unit_systems) > 1:
                for unit_system in unit_systems:
                    unit_subset = subset.loc[subset['Method_UnitSystem'] == unit_system]
                    unit_subset.drop(columns=['Method_UnitSystem'], axis=1, inplace=True)
                    if unit_system != 'English':
                        sql_table = f"{table_name}_{unit_system}_Sample"
                    else:
                        sql_table = f"{table_name}_Sample"
                    self._data_map[sql_table] = unit_subset
            else:
                sql_table = f"{table_name}_Sample"
                subset.drop(columns=['Method_UnitSystem'], axis=1, inplace=True)
                self._data_map[sql_table] = subset

    def _process_events(self):

        def parse_list_val(val):
            if (val is not None) and (str(val) != 'nan') and str(val) != '' and str(val) != ' ':
                comma_parse = val.split(',')
                comma_items = len(comma_parse)

                space_parse = val.split(' ')
                space_items = len(space_parse)

                slash_parse = val.split('/')
                slash_items = len(slash_parse)

                if (comma_items == space_items and comma_items > 1) or (comma_items > 1 and space_items > 0):
                    return [x.strip() for x in comma_parse]
                elif comma_items == 1 and space_items > 1:
                    return [x.strip() for x in space_parse]
                elif slash_items > 1:
                    return [x.strip() for x in slash_parse]
                else:
                    return [x.strip() for x in comma_parse]
            else:
                return []

        def combine_teams(row, return_field):
            duff_field = row['DuffFieldTeam']
            duff_entry = row['DuffEntryTeam']
            hr_field = row['HrFieldTeam']
            hr_entry = row['HrEntryTeam']
            fine_field = row['FineFieldTeam']
            fine_entry = row['FineEntryTeam']
            veg_field = row['VegFieldTeam']
            veg_entry = row['VegEntryTeam']
            trees_field = row['TreesFieldTeam']
            trees_entry = row['TreesEntryTeam']
            sap_field = row['SapFieldTeam']
            sap_entry = row['SapEntryTeam']
            seed_field = row['SeedFieldTeam']
            seed_entry = row['SeedEntryTeam']

            if return_field == 'FuelsObserver':
                fuels_field = ', '.join(
                    list(set(
                        parse_list_val(duff_field) +
                        parse_list_val(hr_field) +
                        parse_list_val(fine_field) +
                        parse_list_val(veg_field)
                    ))
                )
                return fuels_field
            elif return_field == 'FuelsRecorder':
                fuels_entry = ', '.join(
                    list(set(
                        parse_list_val(duff_entry) +
                        parse_list_val(hr_entry) +
                        parse_list_val(fine_entry) +
                        parse_list_val(veg_entry)
                    ))
                )
                return fuels_entry
            elif return_field == 'TreeObserver':
                all_tree_field = ', '.join(
                    list(set(
                        parse_list_val(trees_field) +
                        parse_list_val(sap_field) +
                        parse_list_val(seed_field)
                    ))
                )
                return all_tree_field
            elif return_field == 'TreeRecorder':
                all_tree_entry = ', '.join(
                    list(set(
                        parse_list_val(trees_entry) +
                        parse_list_val(sap_entry) +
                        parse_list_val(seed_entry)
                    ))
                )
                return all_tree_entry

        self['SurfaceFuels_Duff_Litter_Sample']['DuffFieldTeam'] = self['SurfaceFuels_Duff_Litter_Sample']['FieldTeam']
        self['SurfaceFuels_Duff_Litter_Sample']['DuffEntryTeam'] = self['SurfaceFuels_Duff_Litter_Sample']['EntryTeam']

        self['SurfaceFuels_1000Hr_Sample']['HrFieldTeam'] = self['SurfaceFuels_1000Hr_Sample']['FieldTeam']
        self['SurfaceFuels_1000Hr_Sample']['HrEntryTeam'] = self['SurfaceFuels_1000Hr_Sample']['EntryTeam']

        self['SurfaceFuels_Fine_Sample']['FineFieldTeam'] = self['SurfaceFuels_Fine_Sample']['FieldTeam']
        self['SurfaceFuels_Fine_Sample']['FineEntryTeam'] = self['SurfaceFuels_Fine_Sample']['EntryTeam']

        self['SurfaceFuels_Vegetation_Sample']['VegFieldTeam'] = self['SurfaceFuels_Vegetation_Sample']['FieldTeam']
        self['SurfaceFuels_Vegetation_Sample']['VegEntryTeam'] = self['SurfaceFuels_Vegetation_Sample']['EntryTeam']

        self['Trees_Individuals_Sample']['TreesFieldTeam'] = self['Trees_Individuals_Sample']['FieldTeam']
        try:
            self['Trees_Individuals_Sample']['TreesEntryTeam'] = self['Trees_Individuals_Sample']['EntryTeam']
        except KeyError:
            self['Trees_Individuals_Sample']['TreesEntryTeam'] = self['Trees_Individuals_Sample']['FieldTeam']

        self['Trees_Saplings_DiameterClass_Sample']['SapFieldTeam'] = self['Trees_Saplings_DiameterClass_Sample']['FieldTeam']
        try:
            self['Trees_Saplings_DiameterClass_Sample']['SapEntryTeam'] = self['Trees_Saplings_DiameterClass_Sample']['EntryTeam']
        except KeyError:
            self['Trees_Saplings_DiameterClass_Sample']['SapEntryTeam'] = self['Trees_Saplings_DiameterClass_Sample'][
                'FieldTeam']

        self['Trees_Seedlings_HeightClass_Sample']['SeedFieldTeam'] = self['Trees_Seedlings_HeightClass_Sample']['FieldTeam']
        try:
            self['Trees_Seedlings_HeightClass_Sample']['SeedEntryTeam'] = self['Trees_Seedlings_HeightClass_Sample']['EntryTeam']
        except KeyError:
            self['Trees_Seedlings_HeightClass_Sample']['SeedEntryTeam'] = self['Trees_Seedlings_HeightClass_Sample'][
                'FieldTeam']

        temp_events = self['SampleEvent'] \
            .merge(self['MacroPlot'], left_on='SampleEvent_Plot_GUID', right_on='MacroPlot_GUID', how='left') \
            .merge(self['SurfaceFuels_Duff_Litter_Sample'], left_on='SampleEvent_GUID',
                   right_on='SampleData_SampleEvent_GUID', how='left') \
            .merge(self['SurfaceFuels_1000Hr_Sample'], left_on='SampleEvent_GUID',
                   right_on='SampleData_SampleEvent_GUID', how='left') \
            .merge(self['SurfaceFuels_Fine_Sample'], left_on='SampleEvent_GUID',
                   right_on='SampleData_SampleEvent_GUID', how='left') \
            .merge(self['SurfaceFuels_Vegetation_Sample'], left_on='SampleEvent_GUID',
                   right_on='SampleData_SampleEvent_GUID', how='left') \
            .merge(self['Trees_Individuals_Sample'], left_on='SampleEvent_GUID',
                   right_on='SampleData_SampleEvent_GUID', how='left') \
            .merge(self['Trees_Saplings_DiameterClass_Sample'], left_on='SampleEvent_GUID',
                   right_on='SampleData_SampleEvent_GUID', how='left') \
            .merge(self['Trees_Seedlings_HeightClass_Sample'], left_on='SampleEvent_GUID',
                   right_on='SampleData_SampleEvent_GUID', how='left')
        temp_events['FuelsObserver'] = temp_events.apply(combine_teams, args=('FuelsObserver',), axis=1)
        temp_events['FuelsRecorder'] = temp_events.apply(combine_teams, args=('FuelsRecorder',), axis=1)
        temp_events['TreeObserver'] = temp_events.apply(combine_teams, args=('TreeObserver',), axis=1)
        temp_events['TreeRecorder'] = temp_events.apply(combine_teams, args=('TreeRecorder',), axis=1)

        # sel_events = temp_events[['EventID', 'PlotID', 'SampleEvent_Date', 'SampleEvent_GUID',
        #                           'SampleEvent_Comment', 'SampleEvent_Who', 'TreeObserver', 'TreeRecorder',
        #                           'FuelsObserver', 'FuelsRecorder']]
        self._data_map['SampleEvent'] = temp_events

    def _process_projects(self):

        temp_df = self['MonitoringStatus'] \
            .merge(self['MM_MonitoringStatus_SampleEvent'],
                   how='left',
                   left_on='MonitoringStatus_GUID',
                   right_on='MM_MonitoringStatus_GUID') \
            .merge(self['SampleEvent'],
                   how='left',
                   left_on='MM_SampleEvent_GUID',
                   right_on='SampleEvent_GUID') \
            .merge(self['ProjectUnit'],
                   how='left',
                   left_on='MonitoringStatus_ProjectUnit_GUID',
                   right_on='ProjectUnit_GUID')

        temp_df['VisitYear'] = pd.DatetimeIndex(temp_df['SampleEvent_Date']).year
        temp_df['VisitID'] = temp_df.apply(lambda row: row['ProjectID'] + (
                                                        str(int(row['VisitYear']))
                                                        if not pd.isna(row['VisitYear']) else ''
                                                        ) +
                                                       str(row['MonitoringStatus_Prefix']).strip(' ') +
                                                       (
                                                            str(row['MonitoringStatus_Base']).strip(' ')
                                                            if row['MonitoringStatus_Base'] == 'Fire' else ''
                                                       ) +
                                                       (
                                                           str(row['MonitoringStatus_Suffix']).strip(' ')
                                                           if (not pd.isna(row['MonitoringStatus_Suffix'])) and (row['MonitoringStatus_Suffix'] != 'Immediate')
                                                           else
                                                               str(row['MonitoringStatus_Suffix'])[:3]
                                                               if row['MonitoringStatus_Suffix'] == 'Immediate'
                                                               else ''
                                                        ),
                                           axis=1)

        event_df = self['SampleEvent'] \
            .merge(temp_df[['MM_SampleEvent_GUID', 'VisitID']],
                   how='left',
                   left_on='SampleEvent_GUID',
                   right_on='MM_SampleEvent_GUID')

        self._data_map['SampleEvent'] = event_df
        self._data_map['ProjectVisit'] = temp_df

    def format_tables(self):

        self._attr_to_many()
        self._sample_to_many()

        # need to normalize project names for ProjectID
        self['ProjectUnit']['ProjectID'] = self['ProjectUnit'].apply(
            lambda row: row['ProjectUnit_Name'].replace('_', '').replace(' ', ''),
            axis=1
        )

        # add admin unit for data quality
        self['ProjectUnit']['AdminUnit'] = self.admin_unit
        self['MacroPlot']['AdminUnit'] = self.admin_unit

        # Create transects from SurfaceFuels_Fine_Attribute
        temp_df = self['SurfaceFuels_Fine_Attribute'][['EventID', 'Transect', 'Azimuth', 'Slope']].drop_duplicates()
        temp_df['Length'] = 75
        self['Transect'] = temp_df

        self._process_events()
        self._process_projects()

        del self._data_map['SampleData']
        del self._data_map['SampleRow']
        del self._data_map['AttributeRow']
        del self._data_map['AttributeData']

        self.many_tables = True

    def _insert_into_db(self, ffi_db, table):
        """
        Checks foreign key constraints and inserts any necessary tables first.

        Then generates a MERGE INTO statement directly creating a query with the data values and executes that statement
        """

        # Next block pulls in the field and table mappings that were built for the old column and table names to align
        # with the new ones.
        table_map = pd.read_csv("extra/TableMap.csv")
        field_map = pd.read_csv("extra/FieldMap.csv")
        field_map['TableName'] = field_map.apply(lambda r: r['TableName'].strip(), axis=1)
        field_map['OldColumn'] = field_map.apply(lambda r: str(r['OldColumn']).strip(), axis=1)
        field_map['ColumnName'] = field_map.apply(lambda r: r['ColumnName'].strip(), axis=1)

        this_table_map = table_map.loc[table_map['FFITable'] == table]
        if not this_table_map.empty:
            new_table_name = this_table_map['NewTable'].values[0]
            table_name = new_table_name

            temp_field_map = field_map.loc[field_map['TableName'] == table_name]
            this_field_map = dict(zip(list(temp_field_map['OldColumn']), list(temp_field_map['ColumnName'])))
            table_fields = [col for col in list(temp_field_map['OldColumn']) if col != 'nan']

            # handle key constraints
            pks = ffi_db.get_primary_keys()
            fks = ffi_db.get_foreign_keys()

            table_fks = fks[table_name]
            table_pks = pks[table_name]
            # multi_pk = len(table_pks) > 1

            # we need to ensure that the tables on which there are foreign key constraints are entered before we upload
            # new data to the current table. This will produce a recursive pattern to insert all dependencies first.
            if len(table_fks) > 0:
                for const in table_fks:
                    const_list = table_fks[const]
                    for tup in const_list:
                        add_table = tup[0]
                        if add_table not in self._processed and \
                                add_table in self._data_map:
                            print(f'Adding foreign key dependency: {add_table}')
                            self._insert_into_db(ffi_db, add_table)

            xml_table = self[table]
            select_fields = [field for field in table_fields if field in xml_table.columns]
            final_table = xml_table[select_fields].copy()

            if table == 'ProjectVisit':
                final_table.drop_duplicates(inplace=True)
            # for k in table_pks:
            #     if k == 'ID':  # we need to make sure the types are preserved
            #         xml_table[k] = xml_table[k].astype('int64')

            # Construct the VALUES part of the statement
            val_list = []
            col_list = []
            null_cols = []

            # enumerate rows and each value in each row
            for _, row in final_table.iterrows():
                row_vals = []
                for idx, val in enumerate(row):
                    col = final_table.columns[idx]
                    col_type = final_table[col].dtype
                    new_col = this_field_map[col]

                    if new_col not in col_list:
                        col_list.append(new_col)

                    # make sure strings get tick marks and ' is converted to '' for the SQL
                    if col_type in ['float64', 'int64', 'boolean']:
                        row_vals.append(str(val))
                    elif (val is None) or (str(val) == 'nan') or (new_col == 'Offset' and val in ['False', 'True']):
                        row_vals.append('NULL')
                    else:
                        row_vals.append(f"""'{str(val).replace("'", "''")}'""")
                val_list.append(f"({', '.join(row_vals)})")

            values_part = ', '.join(val_list)

            # constructs comma-delimited lists of column names
            cols_str = ', '.join(col_list)
            source_cols = [f'source.{c}'
                           for c in col_list]
            source_col_str = ', '.join(source_cols)

            # Constructs identity relations for primary keys
            pk_strings = [f'target.{pk} = source.{pk}'
                          for pk in table_pks]
            pk_part = ' AND '.join(pk_strings)

            # Generate the full MERGE INTO statement
            merge_into_sql = f"""
            MERGE INTO {table_name} AS target
            USING (
                VALUES
                    {values_part}
                )
                AS source ({cols_str})
                ON {pk_part}
            WHEN NOT MATCHED THEN
                INSERT ({cols_str})
                VALUES ({source_col_str});
            """

            with ffi_db.start_session() as sesh:
                try:
                    count_sql = f"SELECT COUNT(*) AS Size FROM {table_name}"
                    before_df = pd.read_sql(count_sql, sesh.bind)
                    before_count = before_df['Size'].values[0]
                    sesh.execute(merge_into_sql)
                    sesh.commit()

                    after_df = pd.read_sql(count_sql, sesh.bind)
                    after_count = after_df['Size'].values[0]

                    count_diff = after_count - before_count
                    if count_diff != 0:
                        change_type = "INSERT" if count_diff > 0 else "DELETE"
                        dt = str(datetime.datetime.now())
                        new_dt = re.findall(r'(.*)\.\d{4}', dt)[0]
                        change_df = DataFrame({'User': [self._user],
                                               'Time': [new_dt],
                                               'Table': [table_name],
                                               'ChangeType': [change_type],
                                               'Changes': [abs(count_diff)]})
                        change_df.to_sql('UpdateLog', sesh.bind, if_exists='append', index=False)
                    print(f"Inserted {count_diff} rows into {table_name}.")
                    logging.info(f"Inserted {count_diff} rows into {table_name}.")
                except Exception as e:
                    print(f"Failed to insert data into {table_name}.")
                    self.insert_failed.append(table)
                    sesh.rollback()
                    error = str(e)
                    if len(error) > 0:
                        print(error)
                        logging.exception(f"Failed to insert data for {table_name}.")

    def tables_to_db(self, ffi_db):
        """
        Iterates through each table in the data map and inserts it into the database
        """
        print(f'Inserting data for {self.file}')
        logging.info(f"Inserting data for {self.file}")
        for table in self._data_map:
            if table not in self._excluded:
                self._insert_into_db(ffi_db, table)

    def tables_to_csv(self):

        if not os.path.isdir('csv'):
            os.mkdir('csv')

        for table in self._data_map:
            df = self._data_map[table]
            df.to_csv(f'csv/{table}.csv')



