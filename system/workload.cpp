#include "global.h"
#include "helper.h"
#include "workload.h"
#include "row.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"

RC workload::init() {
    sim_done = false;
    return RCOK;
}

RC workload::init_schema(std::istream &in) {
    assert(sizeof(uint64_t) == 8);
    assert(sizeof(double) == 8);
    string line;
    Catalog * schema;
    uint32_t num_indexes = 0;
    uint32_t num_tables = 0;

    while (getline(in, line)) {
        if (line.compare(0, 6, "TABLE=") == 0) {
            string tname;
            tname = &line[6];
            schema = (Catalog *) _mm_malloc(sizeof(Catalog), 64);
            getline(in, line);
            int col_count = 0;
            // Read all fields for this table.
            vector<string> lines;
            while (line.length() > 1) {
                lines.push_back(line);
                getline(in, line);
            }
            schema->init( tname.c_str(), lines.size() );
            for (uint32_t i = 0; i < lines.size(); i++) {
                string line = lines[i];
                size_t pos = 0;
                string token;
                int elem_num = 0;
                int size = 0;
                string type;
                string name;
                while (line.length() != 0) {
                    pos = line.find(",");
                    if (pos == string::npos)
                        pos = line.length();
                    token = line.substr(0, pos);
                    line.erase(0, pos + 1);
                    switch (elem_num) {
                    case 0: size = atoi(token.c_str()); break;
                    case 1: type = token; break;
                    case 2: name = token; break;
                    default: assert(false);
                    }
                    elem_num ++;
                }
                assert(elem_num == 3);
                schema->add_col((char *)name.c_str(), size, (char *)type.c_str());
                col_count ++;
            }
            table_t * cur_tab = (table_t *) _mm_malloc(sizeof(table_t), 64);
            cur_tab->init(schema);
            cur_tab->set_table_id( num_tables );
            num_tables ++;
            tables.push_back( cur_tab );
        } else if (!line.compare(0, 6, "INDEX=")) {
            string iname;
            iname = &line[6];
            getline(in, line);

            vector<string> items;
            string token;
            size_t pos;
            while (line.length() != 0) {
                pos = line.find(",");
                if (pos == string::npos)
                    pos = line.length();
                token = line.substr(0, pos);
                items.push_back(token);
                line.erase(0, pos + 1);
            }

            string tname(items[0]);
            INDEX * index = (INDEX *) _mm_malloc(sizeof(INDEX), 64);
            new(index) INDEX();
            int part_cnt = 1;
            uint32_t table_id = index_to_table(num_indexes);
#if INDEX_STRUCT == IDX_HASH
            if (WORKLOAD == YCSB)
                index->init(tables[ table_id ] , g_synth_table_size * 2);
            else if (WORKLOAD == TPCC) {
                assert(tables[table_id] != NULL);
                index->init(tables[table_id], stoi( items[1] ) * part_cnt);
            }
#else
            index->init(part_cnt, tables[table_id]);
#endif
            index->set_index_id(num_indexes);
            num_indexes ++;
            indexes.push_back(index);
        }
    }
    return RCOK;
}

void workload::index_insert(INDEX * index, uint64_t key, row_t * row) {
    RC rc = index->insert(key, row);
    M_ASSERT( rc == RCOK, "rc=%d\n", rc);
}
