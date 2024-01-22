#pragma once

#include <iostream>
#include <chrono>
#include <ctime>
#include <fstream>
#include <sstream>
#include <stdio.h>
// #include <stdlib.h>
#include <string.h>
#include <set>
#include <vector>
#include <string>
#include <random>
#include <time.h>
#include <cstdlib>
#include <vector>
#include <map>

double random_01() {
    // std::random_device dev;
    // std::mt19937 rng(dev());
    // std::uniform_int_distribution<std::mt19937::result_type> dist6(1, 6);
    // return dist6(rng);
    srand(time(0));
    int next = rand() % 100;
    return next*0.010;
}

class CSVRow
{
    public:
        double operator[](std::size_t index) const
        {
            return std::stod(m_line.substr(m_data[index] + 1, m_data[index + 1]));
        }
        std::size_t size() const
        {
            return m_data.size() - 1;
        }
        void readNextRow(std::istream& str)
        {
            std::getline(str, m_line);

            m_data.clear();
            m_data.emplace_back(-1);
            std::string::size_type pos = 0;
            while((pos = m_line.find(',', pos)) != std::string::npos)
            {
                m_data.emplace_back(pos);
                ++pos;
            }
            // This checks for a trailing comma with no data after it.
            pos   = m_line.size();
            m_data.emplace_back(pos);
        }
    private:
        std::string         m_line;
        std::vector<int>    m_data;
};

std::istream& operator>>(std::istream& str, CSVRow& data)
{
    data.readNextRow(str);
    return str;
}


namespace bench {
struct Schema {
public:
    std::map<std::string, int32_t> schema2id;
    std::vector<std::string> attrs;

    std::string name;
    Schema(std::vector<std::string> A) {
        attrs = A;
        for(std::string s: A) {
            schema2id.insert(std::pair<std::string, int32_t>(s, schema2id.size()));
        }
    }
    Schema(std::vector<std::string> A, std::string table_name) {
        attrs = A; name = table_name;
        for(std::string s: A) {
            schema2id.insert(std::pair<std::string, int32_t>(s, schema2id.size()));
        }
    }
    // Schema(std::string *A, size_t length, std::string table_name) {
    //     name = table_name;
    //     for(int i=0;i<length;i++) {
    //         attrs.push_back(A[i]);
    //         schema2id.insert(std::pair<std::string, int32_t>(A[i], schema2id.size()));
    //     }
    // }
    int32_t position(std::string A) { return schema2id.find(A)->second; }
    std::vector<std::string> attributes() { return attrs; }
};

class parameter {
public:
    std::string path;// = "/com.docker.devenvironments.code/iot.climate.csv";
    std::vector<bench::Schema> schemas;
    std::vector<int32_t> gen; // MS
    std::vector<std::string> sql;
    std::vector<std::string> clean;
    long time_m = 0x7f7f7f7f;
    long time_M = 0;
    std::vector<double> val_m;
    std::vector<double> val_M;
    parameter() {}
};

class Constants {
public:
    const std::string LOG_PATH_POSTFIX = "-log.txt";
    const std::string RES_PATH_POSTFIX = "-res.txt";
    parameter climate;
    parameter ship;
    parameter bitcoin;
    parameter noise;
    Constants() {
        climate::build_parameter(climate);
        ship::build_parameter(ship);
        bitcoin::build_parameter(bitcoin);
        noise::build_parameter(noise);
    }
};
}





namespace climate {
void build_parameter(bench::parameter &param) {
    param.path = "/com.docker.devenvironments.code/iot.climate.csv";
    // std::vector<bench::Schema> schemas;
    // std::vector<int32_t> gen; // MS
    // std::vector<std::string> sql;
    param.sql.push_back("create table climate1 (time integer not null, A numeric(32, 4) not null," +
                    "B numeric(32, 4) not null, primary key(time) );");
    param.sql.push_back("create table climate2 (time integer not null, C numeric(32, 4) not null," +
                    "D numeric(32, 4) not null, primary key(time) );");
    param.sql.push_back("create branch repair from master;");
    std::vector<std::string> climate_attr1 = {"A", "B"};
    bench::Schema climate1 = bench::Schema(climate_attr1, "climate1");
    std::vector<std::string> climate_attr2 = {"C", "D"};
    bench::Schema climate2 = bench::Schema(climate_attr2, "climate2");
    param.schemas.push_back(climate1);
    param.schemas.push_back(climate1);
    param.gen.push_back(2); param.gen.push_back(5);
    for(int i=0;i<4;i++) { param.val_M.push_back(0.0d); param.val_m.push_back(100000000.0d); }
}
}


namespace ship {
void build_parameter(bench::parameter &param) {
param.path = "/com.docker.devenvironments.code/iot.ship.csv";
// std::vector<bench::Schema> schemas;
// std::vector<int32_t> gen;
    for(int i=0;i<10;i++) {
        std::vector<std::string> ship_attr;
        std::string create = "create table ship"+std::to_string(i) + "( time int not null";
        for(int j=0;j<21;j++) {
            std::string attr = std::to_string(i) + "x" + std::to_string(j);
            ship_attr.push_back(attr);
            create += "," + attr + " numeric(32, 4) not null";
        }
        create += ", primary key (time) );";
        bench::Schema shipx = bench::Schema(ship_attr, "ship"+std::to_string(i));
        param.schemas.push_back(shipx);
        param.sql.push_back(create);
    }
    param.sql.push_back("create branch repair from master;");
    param.gen.push_back(1); param.gen.push_back(2); param.gen.push_back(1); param.gen.push_back(2);
    param.gen.push_back(3);
    param.gen.push_back(2); param.gen.push_back(5); param.gen.push_back(10); param.gen.push_back(20);
    param.gen.push_back(60);
    for(int i=0;i<210;i++) { param.val_M.push_back(0.0d); param.val_m.push_back(100000000.0d); }
}
}

namespace bitcoin {
void build_parameter(bench::parameter &param) {// 7
    param.sql.push_back("create table bc1 (time int not null, A numeric(32, 4) not null, " +
                "B numeric(32, 4) not null, C numeric(32, 4) not null, primary key(time) );");
    param.sql.push_back("create table bc1 (time int not null, D numeric(32, 4) not null, " +
                "E numeric(32, 4) not null, F numeric(32, 4) not null, G numeric(32, 4) not null, primary key(time) );");
    param.sql.push_back("create branch repair from master;");
    std::vector<std::string> bc_attr1 = {"A", "B", "C"};
    bench::Schema bc1 = bench::Schema(bc_attr1, "bc1");
    std::vector<std::string> bc_attr2 = {"D", "E", "F", "G"};
    bench::Schema bc2 = bench::Schema(bc_attr2, "bc2");
    param.schemas.push_back(bc1);
    param.schemas.push_back(bc1);
    param.gen.push_back(2); param.gen.push_back(5);
    for(int i=0;i<7;i++) { param.val_M.push_back(0.0d); param.val_m.push_back(100000000.0d); }
    //0,11.0,18.0,0.00833333333333333,1.0,0.0,2.0,100050000.0
}
}

namespace noise {
void build_parameter(bench::parameter &param) { // 7
    param.sql.push_back("create table n1 (time integer not null, A numeric(32, 4) not null," +
                    "B numeric(32, 4) not null, primary key(time) );");
    param.sql.push_back("create table n2 (time integer not null, C numeric(32, 4) not null," +
                    "D numeric(32, 4) not null, primary key(time) );");
    param.sql.push_back("create branch repair from master;");
    std::vector<std::string> bc_attr1 = {"A", "B"};
    bench::Schema bc1 = bench::Schema(bc_attr1, "n1");
    std::vector<std::string> bc_attr2 = {"D", "E"};
    bench::Schema bc2 = bench::Schema(bc_attr2, "n2");
    param.schemas.push_back(bc1);
    param.schemas.push_back(bc1);
    param.gen.push_back(2); param.gen.push_back(5);
    for(int i=0;i<4;i++) { param.val_M.push_back(0.0d); param.val_m.push_back(100000000.0d); }
}
}
