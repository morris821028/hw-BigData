#include <bits/stdc++.h>
using namespace std;

int main() {
    srand(time(NULL));
	int testcase, n, m, x, y;
	testcase = 1;
	while (testcase--) {
        n = 500, m = 4096;
        vector<string> A;
        for (int i = 0; i < n; i++) {
            printf("%03d##", i);
            string x = "";
            if (i == 0 || rand()%n < n/10) {
                for (int j = 0; j < m; j++)
                    x += (char) (rand()%2 + '0');
                printf("%s\n", x.c_str());
            } else {
                x = A[rand()%i];
                int diff = rand()% (m / 20);
                for (int j = 0; j < m; j++) {
                    if (rand()%20 == 0 && diff > 0) {
                        diff--;
                        x[j] = x[j] == '0' ? '1' : '0';
                    }
                }
                printf("%s\n", x.c_str());
            }
            A.push_back(x);
        }
	}
	return 0;
}