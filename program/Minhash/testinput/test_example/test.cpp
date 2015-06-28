#include <bits/stdc++.h>
using namespace std;

vector< pair<int, string> > A;
vector< pair<int, vector<string> > > B;
int main(int argc, char* argv[]) {
    assert(argc == 3);
    fstream fin1(argv[1]), fin2(argv[2]);
    string line;
    while (getline(fin1, line)) {
        for (int i = 0; i < line.length(); i++) {
            if (line[i] == '#')
                line[i] = ' ';
        }
        stringstream sin(line);
        string text;
        int id;
        sin >> id >> text;
        A.push_back(make_pair(id, text));
    }

    while (getline(fin2, line)) {
        stringstream sin(line);
        string text;
        int id;
        vector<string> sign;
        sin >> id;
        while (sin >> text)
            sign.push_back(text);
        B.push_back(make_pair(id, sign));
    }
    sort(A.begin(), A.end());
    sort(B.begin(), B.end());
    
    assert(A.size() == B.size());
    
    int f[2][2] = {}, LSH_f[2][2] = {};
    const double threshold = 0.8;
    // LSH config
    const int BAND_PER = 4;
    const int ROW = B[0].second.size();
    
    fprintf(stderr, "find similar %.2lf%% pair\n", threshold * 100);
    fprintf(stderr, "#HASH ROW %d\n", ROW);
    fprintf(stderr, "#BAND PER ROW %d\n", BAND_PER);
    fprintf(stderr, "\n");
    
    for (int i = 0; i < A.size(); i++) {
        for (int j = i+1; j < A.size(); j++) {
            double same = 0, union_s = 0, htest = 0;
            for (int k = 0; k < A[i].second.length(); k++)
                same += A[i].second[k] == '1' && A[j].second[k] == '1',
                union_s += A[i].second[k] == '1' || A[j].second[k] == '1';
            same /= union_s;
            
            for (int k = 0; k < B[i].second.size(); k++)
                htest += B[i].second[k] == B[j].second[k];
            htest /= B[i].second.size();
            
            int same_band = 0;
            for (int l = 0, r; l < B[i].second.size(); l = r+1) {
                r = min(l + BAND_PER, (int) B[i].second.size() - 1);
                int ok = 1;
                for (int a = l; a <= r && ok; a++)
                    ok &= B[i].second[a] == B[j].second[a];
                same_band += ok;
            }
            
            if (htest >= threshold) {
                if (same >= threshold)
                    f[1][1]++;
                else
                    f[1][0]++;
            } else {
                if (same >= threshold)
                    f[0][1]++;
                else
                    f[0][0]++;
            }
            
            if (same_band >= 1) {
                if (same >= threshold)
                    LSH_f[1][1]++;
                else
                    LSH_f[1][0]++;
            } else {
                if (same >= threshold)
                    LSH_f[0][1]++;
                else
                    LSH_f[0][0]++;
            }
        }
    }
    
    fprintf(stderr, "Simple compare O(n^2)\n\n");
    fprintf(stderr, "|Judge\\Real|  True   |   False  |\n");
    fprintf(stderr, "|----------|---------|----------|\n");
    fprintf(stderr, "|  True    |  %6d |  %6d  |\n", f[1][1], f[1][0]);
    fprintf(stderr, "| False    |  %6d |  %6d  |\n", f[0][1], f[0][0]);
    
    fprintf(stderr, "\n");
    
    fprintf(stderr, "LSH compare O(n^2), at least 1 common bucket, using HASH will O(1), check same bucket.\n\n");
    fprintf(stderr, "|Judge\\Real|  True   |   False  |\n");
    fprintf(stderr, "|----------|---------|----------|\n");
    fprintf(stderr, "|  True    |  %6d |  %6d  |\n", LSH_f[1][1], LSH_f[1][0]);
    fprintf(stderr, "| False    |  %6d |  %6d  |\n", LSH_f[0][1], LSH_f[0][0]);
	return 0;
}