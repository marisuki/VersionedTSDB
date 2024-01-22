#include <llvm/ADT/STLExtras.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/GenericValue.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/Support/ManagedStatic.h>
#include <llvm/Support/TargetSelect.h>

#include "foundations/Database.hpp"
#include "queryCompiler/queryCompiler.hpp"
#include <readline/readline.h>
#include <readline/history.h>

#include "benchmark.hpp"



void init(Database &currentdb, bench::parameter param, int extract_from,
int extract_end, int insert_size, double duplicate, double repair, double delays) {
    bench::Constants constx;
    std::string LOG_PATH = std::to_string(time(0)) + "Tardis-insert" + constx.LOG_PATH_POSTFIX;

    auto start = std::chrono::steady_clock::now();
    // std::string create = "create table " + schema.name + "(time int not null";
    // for(std::string attr: ) {
    //     create.append("," + attr + " numeric(32, 4) not null");
    // }
    // create.append("primary key ());");
    for(std::string qi: param.sql) {
        QueryCompiler::compileAndExecute(qi.c_str(),*currentdb);
    }

    std::string line;
    std::ifstream infile(param.path);
    std::vector<std::string> update;
    CSVRow row;
    int written_repair = 0;
    int32_t inserted = 0;
    while(infile >> row && inserted <= insert_size) {
        int col = 1;
        for(int i=0;i<param.schemas.size();i++) {
            bench::Schema schema = param.schemas.at(i);
            int timex = inserted*param.gen.at(i);
            param.time_M = std::max(param.time_M, timex);
            param.time_m = std::min(param.time_m, timex);
            std::string x = " values (" + std::to_string(timex);
            for(int j=0;j<schema.attributes.size();j++) {
                double val = std::stod(row[col], 10);
                param.val_M.at(col) = std::max(param.val_M.at(col), val);
                param.val_m.at(col) = std::min(param.val_m.at(col), val);
                x += "," + row[col++];
            }
            x += ");";
            QueryCompiler::compileAndExecute(("insert into " + schema.name + " version master " + x).c_str(), *currentdb);
            if(random_01() < repair) {
                written_repair ++;
                update.push_back("insert into " + schema.name + " version repair " + x);
            }
        }
    }
    infile.close();
    auto time_cost1 = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count();
    start = std::chrono::steady_clock::now();
    for(auto q: update) {
        QueryCompiler::compileAndExecute(q.c_str(), *currentdb);
    }
    auto time_cost2 = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count();
    std::ofstream outfile(LOG_PATH);
    fprintf(outfile, "Insertion time1 cost(MS)=%ld\nInsertion time2 cost(MS)=%ld\nInserted=%d\nRepaired=%d\n", time_cost1, time_cost2, inserted, written_repair);
    outfile.close();
}

void clean(Database &currentdb) {

}

void alignment(Database &currentdb, bench::parameter param) {
    auto start = std::chrono::system_clock::now();
    std::string sql = "select * from ";
    sql.append(s);
    for(int i=0;i<param.schemas.size();i++) {
        std::string s = param.schemas.at(i).name;
        sql.append(", ");sql.append(s);
    }
    QueryCompiler::compileAndExecute(sql.c_str(),*currentdb);
    auto time_cost2 = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start).count();
}

void rangeFilter(Database &currentdb, bench::parameter param, double selectivity) {
    auto start = std::chrono::system_clock::now();
    std::string sql = "select * from ";
    sql.append(s);
    for(int i=0;i<param.schemas.size();i++) {
        std::string s = param.schemas.at(i).name;
        sql.append(", ");sql.append(s);
        sql.append(" where time > 0 and time < ");
        sql.append(std::to_string((int)(param.time_M*selectivity)));
        sql.append(";");
        QueryCompiler::compileAndExecute(sql.c_str(),*currentdb);
    }
    auto time_cost2 = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start).count();
    printf("%d\n", )
}

void valueFilter(Database &currentdb, bench::parameter param, double selectivity) {
    auto start = std::chrono::system_clock::now();
    std::string sql = "select * from ";
    sql.append(s);
    for(int i=0;i<param.schemas.size();i++) {
        std::string s = param.schemas.at(i);
        sql.append(", ");sql.append(s);
    }
    sql.append(" where time > 0 and time < ");
    sql.append(std::to_string((int)(param.time_M*selectivity)));
    sql.append(";");
    QueryCompiler::compileAndExecute(sql.c_str(),*currentdb);
    auto time_cost2 = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start).count();
}

int main(int argc, char * argv[])
{
    // necessary for the LLVM JIT compiler
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();

    std::unique_ptr<Database> currentdb = std::make_unique<Database>();
    create = "create branch repair from master;";
    QueryCompiler::compileAndExecute(create.c_str(),*currentdb);
    Schema schema()

    init(currentdb, )

    prompt();

    llvm::llvm_shutdown();

    return 0;
}