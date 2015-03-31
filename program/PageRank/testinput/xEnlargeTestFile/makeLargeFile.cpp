#include <stdio.h>
#include <iostream>
#include <vector>
using namespace std;
vector<string> A;
const int copyRatio = 3;
int main() {
	string line;
	while (getline(cin, line))
		A.push_back(line);
	for (int i = 0; i < copyRatio; i++) {
		for (int j = 0; j < A.size(); j++)
			cout << A[j] << endl;
		cout << endl;
	}
	return 0;
}