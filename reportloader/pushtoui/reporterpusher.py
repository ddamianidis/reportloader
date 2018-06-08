from pymongo import MongoClient
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
from pymongo.errors import BulkWriteError
import datetime
import copy
import sys
import paramiko
import re
import twpamonitorclient
from reportloader.utils.mongoidtools import ObjectId
from reportloader.utils.sshclient import SSH_Client
from reportloader.database.mongo import MongoConnector
from reportloader.pushtoui.inventory import inventory
from reportloader.pushtoui.platforms import platforms
from reportloader.pushtoui.publisher_fees import publisher_fees
from reportloader.utils.sshclient import SSH_Client

class GracefulError(Exception):
            pass

class ReporterPusher(SSH_Client):
        
    def __init__(self, platform, date, mode='prod'):
        super().__init__()
        self.reports = MongoConnector('reports').collection
        self.date = date     
        self.platform = platform
        self.publisher_fees = publisher_fees()
        self.inventory = inventory()
        self.platforms = platforms()
        self.reports_table = 'reports_e2e_tests' if mode == 'test' else 'reports_e2e_tests'
        self.insert_values = []
        self.messages = []
        
    def is_float(self, str):
        if str is None:
            return False
        try:
            float(str)
            return True
        except ValueError:
            return False 

    
    def build_inserts(self):
        ID_RX = re.compile(r'^(\d+)_.*$')
        SIZE_RX = re.compile(r'^.*_(\d+[xX]\d+)$')
        
        inventory_dict = self.inventory.get()
        publisher_fees = self.publisher_fees.get()
        platforms_ids = self.platforms.get()
        filters = {"date":self.date} 
        projection = {"date":1, "platform":1, "placement_name":1, "clicks":1, 
                      "resold_impressions":1, "revenue_dict.EUR":1, 
                      "revenue_dict.USD":1}
        
        if self.platform != 'all':
            filters.update({
                "platform": self.platform
                })
        
        rows = self.reports.find(filters, projection)
        self.row_len = 0
        self.id_reject_count = 0
        self.size_reject_count = 0
        self.inventory_reject_count = 0
        self.publisher_fee_reject_count = 0
        self.revenue_reject_count = 0
        self.insert_values_eurohoops = 0
        
        for row in rows:
            #print(row)
            placement_name = row['placement_name']
            platform_name = row['platform']
            
            # ignore appnexus placements
            if platform_name == 'appnexus':
                continue
            
            self.row_len += 1
            if platform_name == 'taboola':
                #m_id =  re.match(r'^.*#(\d+)$', placement_name)
                m_id =  re.match(r'^.*-.(\d+)$', placement_name)
                # old naming convention for taboola
                if m_id is None:
                    m_id =  re.match(r'^.*#(\d+)$', placement_name)
            elif platform_name == 'teads':
                m_id =  re.match(r'^.*_(\d+)$', placement_name)        
            else:
                m_id = ID_RX.match(placement_name)
    
            if m_id is None:
                self.id_reject_count += 1
                self.stream_logger.info('Platform %s. Rejecting "%s". Could_not_parse_placement_id' % (platform_name, placement_name))
                self.messages.append('Platform {0}. Rejecting "{1}". Could_not_parse_placement_id'.format(platform_name, placement_name))
                continue
            else:
                placement_id = m_id.group(1)
                
            m_size = SIZE_RX.match(placement_name)
            
            if m_size is None and platform_name != 'taboola' and platform_name != 'teads':
                self.size_reject_count += 1
                self.stream_logger.info('Platform %s. Rejecting "%s". Could_not_parse_placement_size' % (platform_name, placement_name))
                self.messages.append('Platform {0}. Rejecting "{1}". Could_not_parse_placement_size'.format(platform_name, placement_name))
                continue
            else:
                if platform_name == 'taboola':
                    m_id =  re.match(r'^.*-(.+)$', placement_name)
                    if m_id == None:
                        # old naming convention for taboola
                        if placement_name.startswith('Remnant'):
                            placement_type = 'r'
                        else:
                            placement_type = 'p'    
                    else:        
                        placement_type = m_id.group(1)[0]
                
                    if placement_type == 'r':   
                    #if placement_name.startswith('Remnant'):
                        m_size =  re.match(r'^.*#(\d+[xX]\d+)#.+$', placement_name)
                        if m_size is None:
                            self.size_reject_count += 1
                            self.stream_logger.info('Platform %s. Rejecting "%s". Could_not_parse_placement_size' % (platform_name, placement_name))
                            self.messages.append('Platform {0}. Rejecting "{1}". Could_not_parse_placement_size'.format(platform_name, placement_name))
                            continue
                        placement_size = m_size.group(1)
                    else:    
                        placement_size = 'Content Unit'
                elif platform_name == 'teads':
                    placement_size = 'Magic Ads'
                else:
                    placement_size = m_size.group(1)
            # Get insert row data from dailies view
            insert_row = {
                'date': row['date'],
                'size': placement_size if placement_size else None,
                'imps': row['resold_impressions'] if 'resold_impressions' in row else 0,
                'clicks': row['clicks'] if 'clicks' in row else 0,
                'placement_id': placement_id,
                'revenue_usd': row['revenue_dict']['USD'],
            }
    
            # Update insert row from inventory table
            plc_id_str = str(insert_row['placement_id'])
            if plc_id_str not in inventory_dict:
                self.inventory_reject_count += 1
                self.stream_logger.info('Platform %s. Rejecting "%s". Could_not_find_it_in_inventory_table' % (platform_name, placement_name))
                self.messages.append('Platform {0}. Rejecting "{1}". Could_not_find_it_in_inventory_table'.format(platform_name, placement_name))
                continue
            else:
                insert_row.update(inventory_dict[plc_id_str])
    
            # Check if we have a PA_MENA publisher and insert usd for revenue
            if inventory_dict[plc_id_str]['publisher_name'].startswith('PA_MENA'):
                insert_row['revenue'] = row['revenue_dict']['USD']
            else:
                insert_row['revenue'] = row['revenue_dict']['EUR']
            
            if  not self.is_float(insert_row['revenue']):
                self.revenue_reject_count += 1
                self.stream_logger.info('Platform %s. Rejecting "%s". revenue_is_not_float' % 
                                  (platform_name, placement_name))
                self.messages.append('Platform {0}. Rejecting "{1}". revenue_is_not_float'.format(platform_name, placement_name))
                continue
            
            if insert_row['publisher_id'] not in publisher_fees:
                self.publisher_fee_reject_count += 1
                self.stream_logger.info('Platform {0}. Rejecting "{1}". Could_not_find_fee_for_publisher_id_{2}'.
                                        format(platform_name, placement_name, insert_row['publisher_id']))
                self.messages.append('Platform {0}. Rejecting "{1}". Could_not_find_fee_for_publisher_id_{2}'.
                                        format(platform_name, placement_name, insert_row['publisher_id']))
                continue
            else:
                insert_row['revenue_est_net'] = publisher_fees[insert_row['publisher_id']][0] * float(insert_row['revenue'])
            
            # Update insert row with standard fields, buyer_member_id='2725',
            insert_row.update( {
                        'buyer_member_id': platforms_ids[platform_name] if platform_name != 'appnexus' else None,
                        'brand_id': '17', }
                    )
    
            # Append the insert value statement
            self.insert_values.append("\n('{date}', '{size}', '{imps}', '{clicks}', '{revenue}', '{revenue_usd}', '{revenue_est_net}', '{publisher_id}', '{site_id}', '{placement_id}', '{buyer_member_id}', '{brand_id}')".format(**insert_row))
            
            # patch for Eurohoops
            if insert_row['publisher_id'] == '475297' and insert_row['site_id'] == '2551048':
                self.insert_values_eurohoops += 1 
                insert_row['publisher_id'] = '111111'
                insert_row['revenue_est_net'] = publisher_fees[insert_row['publisher_id']][0] * insert_row['revenue_est_net']
                insert_row['revenue'] = insert_row['revenue_est_net'] / 0.70
                insert_row['revenue_usd'] = ''
                # Append the insert value statement
                self.insert_values.append("\n('{date}', '{size}', '{imps}', '{clicks}', '{revenue}', '{revenue_usd}', '{revenue_est_net}', '{publisher_id}', '{site_id}', '{placement_id}', '{buyer_member_id}', '{brand_id}')".format(**insert_row))
    
    @twpamonitorclient.context('Push Data To UI', 'push_data', {})            
    def push_dailies_to_ui(self, monitor):
        self.stream_logger.info('Begin push_dailies_to_ui')
        
        monitor.log("Start push for platform {0}".format(self.platform))
        
        if self.platform is None:
            self.platform = 'all'
                                    
        # Parse inventory file to get inventory data
        inventory_dict = self.inventory.get()
        #print(inventory_dict)
        # Parse publisher fees file
        publisher_fees = self.publisher_fees.get()
        #print(publisher_fees)
        #exit(0)                                         
        # Parse platforms file
        platforms_ids = self.platforms.get()             
        self.stream_logger.debug('Publisher fees {0}'.format(publisher_fees))
        # Scrap dates from publisher fees dictionary
        publisher_fees = { k: publisher_fees[k][0] for k in publisher_fees}
        self.stream_logger.debug('Publisher fees {0}'.format(publisher_fees))
        self.stream_logger.debug('Platform ids {0}'.format(platforms_ids))
    
        self.build_inserts()
            
        # Create file to upload
        if len(self.insert_values) > 0:
            with open('insert.sql', 'w') as fh:
                fh.write('INSERT INTO {0} ( date, size, imps, clicks, revenue, revenue_usd, revenue_est_net, publisher_id, site_id, placement_id, buyer_member_id, brand_id) VALUES {1};'.format(self.reports_table, ','.join(self.insert_values)))
    
            # Upload to server
            self.put_file('insert.sql','insert.sql')
            try:
                if self.platform == 'all':
                    for pl_name, pl_id in platforms_ids.items():
                        self.do_cmd(self.mysql_prefix + r'-e"DELETE from %s WHERE buyer_member_id=%s AND brand_id=17 AND date=\"%s 00:00:00\" " ' % (self.reports_table, pl_id, self.date,))
                else:
                    if self.platform in platforms_ids:
                        self.do_cmd(self.mysql_prefix + r'-e"DELETE from %s WHERE buyer_member_id=%s AND brand_id=17 AND date=\"%s 00:00:00\" " ' % (self.reports_table, platforms_ids[self.platform], self.date,))
                self.do_cmd(self.mysql_prefix + '< insert.sql')
                #self.do_cmd('/usr/bin/php /var/www/agora/public_html/sync/make_home_mv.php')
            except GracefulError:
                return False
    
        # Finally do the logging
        self.stream_logger.info('Ran for date %s and platform %s' % (self.date, self.platform))
        self.stream_logger.info('Found %s rows in vps database' % self.row_len)
        if self.id_reject_count > 0:
            self.stream_logger.info('Rejected %s rows due to placement id parsing error.' % self.id_reject_count)
        if self.size_reject_count > 0:
            self.stream_logger.info('Rejected %s rows due to size parsing error.' % self.size_reject_count)
        if self.inventory_reject_count > 0:
            self.stream_logger.info('Rejected %s rows due to inventory table lookup error.' % self.inventory_reject_count)
        if self.publisher_fee_reject_count > 0:
            self.stream_logger.info('Rejected %s rows due to publisher fee lookup error.' % self.publisher_fee_reject_count)
        if self.revenue_reject_count > 0:
            self.stream_logger.info('Rejected %s rows due to revenue value error.' % self.revenue_reject_count)    
        self.inserted_values_to_ui = len(self.insert_values)-self.insert_values_eurohoops    
        self.stream_logger.info('Inserted %s rows in agora database' % str(self.inserted_values_to_ui))
        self.stream_logger.info('Project Agora UI update log end')
        self.stream_logger.info('End push_dailies_to_ui')
        
        results = {
            'found': self.row_len,
            'inserted': self.inserted_values_to_ui,
            'inserted_eurohoops': self.insert_values_eurohoops,
            'rejected_id': self.id_reject_count,
            'rejected_size': self.size_reject_count,
            'rejected_inventory': self.inventory_reject_count,
            'rejected_pub_fee': self.publisher_fee_reject_count,
            'rejected_revenue': self.revenue_reject_count,
            'messages':self.messages
        }
        monitor.data({'push_statistics': results})
        monitor.log("End push for platform {0}".format(self.platform))
        return results
