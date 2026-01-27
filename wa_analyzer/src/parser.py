import sqlite3
import pandas as pd
import vobject
import os

class WhatsappParser:
    def __init__(self, msgstore_path, wa_path, vcf_path):
        self.msgstore_path = msgstore_path
        self.wa_path = wa_path
        self.vcf_path = vcf_path
        self.conn_msg = None
        self.conn_wa = None

    def connect(self):
        if os.path.exists(self.msgstore_path):
            self.conn_msg = sqlite3.connect(self.msgstore_path)
            # Handle encoding errors gracefully
            self.conn_msg.text_factory = lambda b: b.decode(errors="ignore")
        if os.path.exists(self.wa_path):
            self.conn_wa = sqlite3.connect(self.wa_path)
            self.conn_wa.text_factory = lambda b: b.decode(errors="ignore")

    def parse_messages(self):
        if not self.conn_msg:
            print("Message store connection not established.")
            return pd.DataFrame()

        # Query based on v3 schema analysis, joining media table and chat subject
        # Added chat.subject to identify group names
        query = """
        SELECT 
            message._id as message_id,
            message.chat_row_id,
            message.from_me,
            message.timestamp,
            message.text_data,
            chat.jid_row_id,
            chat.subject,
            message.message_type,
            message_media.mime_type
        FROM message
        LEFT JOIN chat ON message.chat_row_id = chat._id
        LEFT JOIN message_media ON message._id = message_media.message_row_id
        """
        try:
            df = pd.read_sql_query(query, self.conn_msg)
            # Convert timestamp to datetime (typically ms in WhatsApp)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            print(f"Error parsing messages: {e}")
            return pd.DataFrame()

    def parse_jids(self):
        if not self.conn_msg:
            return pd.DataFrame()
            
        query = """
        SELECT _id as jid_row_id, raw_string, user FROM jid
        """
        try:
            df = pd.read_sql_query(query, self.conn_msg)
            return df
        except Exception as e:
            print(f"Error parsing JIDs: {e}")
            return pd.DataFrame()

    def parse_wa_contacts(self):
        """Parses the wa.db for display names associated with JIDs"""
        if not self.conn_wa:
            print("WA DB connection not established.")
            return pd.DataFrame()

        query = """
        SELECT jid, display_name, wa_name FROM wa_contacts
        """
        try:
            df = pd.read_sql_query(query, self.conn_wa)
            return df
        except Exception as e:
            print(f"Error parsing WA contacts: {e}")
            return pd.DataFrame()

    def parse_vcf(self):
        if not self.vcf_path or not os.path.exists(self.vcf_path):
            return {}

        contacts = {}
        with open(self.vcf_path, 'r') as f:
            for vcard in vobject.readComponents(f):
                try:
                    name = vcard.fn.value
                    # Extract phones
                    if hasattr(vcard, 'tel'):
                        for tel in vcard.tel_list:
                            phone_raw = str(tel.value)
                            # Normalize: keep strict digits
                            digits = "".join(filter(str.isdigit, phone_raw))
                            
                            # Store both full digits and last 9 digits (for matching without country code)
                            if len(digits) > 5:
                                contacts[digits] = name
                                if len(digits) > 9:
                                    contacts[digits[-9:]] = name
                except Exception:
                    pass
        return contacts

    def get_merged_data(self):
        self.connect()
        
        # 1. Get Messages
        messages_df = self.parse_messages()
        if messages_df.empty:
            return pd.DataFrame()

        # 2. Get JIDs (to link chat_row_id -> raw_string)
        jids_df = self.parse_jids()
        
        # 3. Merge JIDs onto messages
        # This brings in 'raw_string' which helps identify groups (ends with g.us)
        merged = pd.merge(messages_df, jids_df, on='jid_row_id', how='left')
        
        # 4. Get Names from WA DB
        wa_contacts_df = self.parse_wa_contacts()
        
        # 5. Get Names from VCF (Dictionary)
        vcf_contacts = self.parse_vcf()
        
        # 6. Resolve Names
        def resolve_name(row):
            # Identifying Groups:
            # If chat.subject exists, it's (almost always) a group or named chat.
            subject = row.get('subject')
            if subject and isinstance(subject, str):
               return subject

            raw_jid = row.get('raw_string')
            if not isinstance(raw_jid, str):
                return "Unknown"
            
            # Additional Group Check via JID suffix
            if raw_jid.endswith("@g.us"):
                return "Unknown Group" # Fallback if subject was null
            
            # Try to match with WA DB first
            if not wa_contacts_df.empty:
                match = wa_contacts_df[wa_contacts_df['jid'] == raw_jid]
                if not match.empty:
                    disp = match.iloc[0]['display_name']
                    if disp: return disp
                    
            # Try to match with VCF
            # raw_jid looks like 34666123456@s.whatsapp.net
            phone_part = raw_jid.split('@')[0]
            
            if phone_part in vcf_contacts:
                return vcf_contacts[phone_part]
            
            # Fuzzy match: check if last 9 digits exist in VCF
            if len(phone_part) > 9:
                suffix = phone_part[-9:]
                if suffix in vcf_contacts:
                    return vcf_contacts[suffix]
                
            # If nothing, use the user field (often just the phone number)
            return row.get('user') or phone_part

        merged['contact_name'] = merged.apply(resolve_name, axis=1)
        
        return merged
