#include <stdio.h> 
#include <math.h>
#include <vector>
#include <string.h>
using namespace std;
// Markov process, math
const int MAXN = 64;
const int MAXIT = 100;
struct Matrix {
    double v[MAXN][MAXN];
    int row, col; // row x col
    Matrix(int n, int m, int a = 0) {
        memset(v, 0, sizeof(v));
        row = n, col = m;
        for(int i = 0; i < row && i < col; i++)
            v[i][i] = a;
    }
    Matrix operator*(const Matrix& x) const {
        Matrix ret(row, x.col);
        for(int i = 0; i < row; i++) {
            for(int k = 0; k < col; k++) {
                if (v[i][k])
                    for(int j = 0; j < x.col; j++) {
                        ret.v[i][j] += v[i][k] * x.v[k][j];
                    }
            }
        }
        return ret;
    }
    Matrix operator+(const Matrix& x) const {
        Matrix ret(row, col);
        for(int i = 0; i < row; i++) {
            for(int j = 0; j < col; j++) {
                ret.v[i][j] = v[i][j] + x.v[i][j];
            }
        }
        return ret;
    }
    Matrix operator^(const int& n) const {
        Matrix ret(row, col, 1), x = *this;
        int y = n;
        while(y) {
            if(y&1)	ret = ret * x;
            y = y>>1, x = x * x;
        }
        return ret;
    }
    Matrix powsum(int k) {
        if (k == 0) return Matrix(row, col, 0);
        Matrix vv = powsum(k/2);
        if (k&1) {
            return vv * (Matrix(row, col, 1) + vv) + vv;
        } else {
            return vv * (Matrix(row, col, 1) + vv);
        }
    }
};

#define eps 1e-6
int cmpZero(double x) {
	if (fabs(x) < eps)
		return 0;
	return x < 0 ? -1 : 1;
}
int main() {
	int N;
	double beta, S;
	while (scanf("%lf %lf", &beta, &S) == 2) {
		vector<int> g[MAXN], invg[MAXN];
		scanf("%d", &N);
		for (int i = 0; i < N; i++) {
			int M, x;
			scanf("%d", &M);
			for (int j = 0; j < M; j++) {
				scanf("%d", &x);
				g[i].push_back(x);
				invg[x].push_back(i);
			}
		}
		
		Matrix r(N, 1);
		for (int i = 0; i < N; i++)
			r.v[i][0] = 1.0 / N;
		
		for (int it = 0; it < MAXIT; it++) {
			Matrix next_r(N, 1);
			for (int i = 0; i < N; i++) {
				for (int j = 0; j < invg[i].size(); j++) {
					int x = invg[i][j];
					next_r.v[i][0] += beta * r.v[x][0] / g[x].size();
				}
			}
			
			double sum = 0;
			for (int i = 0; i < N; i++)
				sum += next_r.v[i][0];
			for (int i = 0; i < N; i++)
				next_r.v[i][0] += (S - sum) / N;
			r = next_r;
		}
		for (int i = 0; i < N; i++)
			printf("%c %.3lf\n", i + 'A', r.v[i][0]);
	}
	return 0;
}
/*
0.7 3
3
2 1 2
1 2
1 2

0.85 3
3
2 1 2
1 2
1 0

1 3
3
2 1 2
1 2
1 0
*/
