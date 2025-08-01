def parse_kegg_entry(self, entry_text):
        import re
        parsed = {}
        current_key = None

        for line in entry_text.splitlines():
            # Convert multiple spaces to a single tab
            line = re.sub(r' {2,}', '\t', line)
            parts = line.split('\t')

            # Skip malformed lines
            if not parts:
                continue

            # If this line starts a new field
            if parts[0].strip():
                current_key = parts[0].strip()

            if current_key == 'ENTRY':
                parsed['ENTRY'] = parts[1].strip() if len(parts) > 1 else None
                parsed['LABEL'] = parts[2].strip() if len(parts) > 2 else None
            if current_key == 'NAME':
                if 'NAME' not in parsed and len(parts) > 1:
                    parsed['NAME'] = parts[1].strip(" ;")
                elif len(parts) > 1:
                    parsed.setdefault('SYNONYMS', []).append(parts[1].strip(" ;"))

            elif current_key in ['PATHWAY', 'MODULE', 'NETWORK'] and len(parts) >= 3:
                parsed.setdefault(current_key, {})[parts[1].strip()] = parts[2].strip()

            elif current_key == 'DBLINKS' and len(parts) > 1:
                parsed.setdefault('DBLINKS', {})
                if ": " in parts[1]:
                    code, info = parts[1].split(": ", 1)
                    parsed['DBLINKS'][code.strip()] = info.strip()

        return parsed


def translate_compounds(self, compounds: list):
        compound_url_base = self.base_url + "/get/"
        parsed_compounds = []
        for compound_batch_idx in range(0, len(compounds), 20):
            compound_batch = compounds[compound_batch_idx:compound_batch_idx + 20]
            compounds_str = "+".join(f"cpd:{cid}" for cid in compound_batch)
            send_url = compound_url_base + compounds_str

            print(f"Fetching KEGG data for compounds: {compounds_str}")
            response = requests.get(send_url)

            if response.ok:
                print(f"{GREEN}Successfully fetched KEGG data for batch: {compounds_str}{RESET}")

                entries = response.text.strip().split("///")
                for entry_text in entries:
                    entry_text = entry_text.strip()
                    if not entry_text:
                        continue

                    parsed = self.parse_kegg_entry(entry_text)
                    print(f"Parsed entry for {parsed.get('ENTRY')}:")
                    parsed_compounds.append(parsed)
            else:
                print(f"{RED}Failed to fetch batch: {compounds_str} — Status: {response.status_code}{RESET}")
        print(f"{GREEN}Finished processing all compound batches.{RESET}")
        return parsed_compounds