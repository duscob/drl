// Copyright 2012 Francisco Claude. All Rights Reserved.

#include <algorithm>
#include <cassert>
#include <fstream>
#include <iostream>
#include <queue>
#include <stack>

using namespace std;

#include <Array.h>
#include <cppUtils.h>
#include <WaveletTreeNoptrs.h>
#include <BitSequenceBuilder.h>
#include <Mapper.h>

using namespace cds_utils;
using namespace cds_static;

class Grammar {
public:
	Grammar(int rules) {
		num_rules_ = rules;
		rules_ = new Array*[num_rules_];
		term_ = new BitString(num_rules_);
	}
	
	~Grammar() {
		delete limits_;
		delete rules_seq_;
		delete term_rs_;
	}

	bool IsTerminal(int r) {
		return term_rs_->access(r) != 0;
	}
	
	int GetRuleLength(int r) {
		if (IsTerminal(r))
			return 0;
		int pos = r;
		int posM1 = r - 1;
		return limits_->getField(r) - ((posM1 >= 0)? limits_->getField(posM1) : 0);
	}

	int GetRule(int r, int o) {
		assert(o < GetRuleLength(r));
		int pos = (r == 0) ? 0 : (limits_->getField(r - 1));
		int ret = rules_seq_->getField(pos + o);
		return ret;
	}

	int GetExpandedLength(int r) {
		if (IsTerminal(r)) return 1;
		int numRules = GetRuleLength(r);
		int sum = 0;
		for (int i = 0; i < numRules; i++)
			sum += GetExpandedLength(GetRule(r, i));
		return sum;
	}

	void Save(ofstream &out) {
		saveValue(out, num_rules_);
		saveValue(out, dummy_term_);
		saveValue(out, initial_);
		limits_->save(out);
		rules_seq_->save(out);
		term_rs_->save(out);
	}

	void SetRule(int i, Array *rule) {
		rules_[i] = rule;
		if (rule == NULL)
			term_->setBit(i);
	}

	int Initial() {
		return initial_;
	}

	int GetSize() {
		int size = sizeof(*this);
		size += rules_seq_->getSize();
		size += limits_->getSize();
		size += term_rs_->getSize();
		return size;
	}

	void Compact() {
		int len = 0;
		for (int i = 0; i < num_rules_; i++) 
			len += (rules_[i] == NULL)? 0 : rules_[i]->getLength();
		limits_ = new Array(num_rules_, len);
		rules_seq_ = new Array(len, num_rules_);
		int pos = 0, limits_pos = 0;
		for (int i = 0; i < num_rules_; i++) {
			
			if (rules_[i] == NULL) {
				limits_->setField(limits_pos++, pos);
				continue;
			}
			for (int j = 0; j < rules_[i]->getLength(); j++) {
				rules_seq_->setField(pos, rules_[i]->getField(j));
				pos++;
			}
			limits_->setField(limits_pos++, pos);
			delete rules_[i];
		}
		delete [] rules_;
		term_rs_ = new BitSequenceRG(*term_, 20);
		delete term_;
	}

	Grammar(ifstream &in) {
		num_rules_ = loadValue<int>(in);
		dummy_term_ = loadValue<int>(in);
		initial_ = loadValue<int>(in);
		limits_ = new Array(in);
		rules_seq_ = new Array(in);
		term_rs_ = BitSequenceRG::load(in);
	}

	void PrintStats() {
		int size = GetSize();
		cout << "\t* Grammar size: " << size << endl;
        cout << "\t\t* n             : " << rules_seq_->getLength() << endl;
        cout << "\t\t* N             : " << limits_->getLength() << endl;
		cout << "\t\t* Rules         : " << rules_seq_->getSize() << endl;
		cout << "\t\t* Limits        : " << limits_->getSize() << endl;
		cout << "\t\t* Terminals     : " << term_rs_->getSize() << endl;
		cout << "\t\t* Overhead      : " << size - rules_seq_->getSize() - term_rs_->getSize() - limits_->getSize() << endl;
	}

	int NumRules() {
		return num_rules_;
	}

	int GetSymbol(int r) {
		assert(IsTerminal(r));
		return term_rs_->rank1(r) - 1;
	}

	bool ShouldIgnore(int v) {
		return v >= dummy_term_;
	}

	static Grammar *ReadPlain(const string &index) {
		// Read index
		ifstream _index(index.c_str());
		int num_rules;
		_index.read((char *)&num_rules, sizeof(int));
		
		Grammar *ret = new Grammar(num_rules);
		_index.read((char *)&ret->dummy_term_, sizeof(int));
		_index.read((char *)&ret->initial_, sizeof(int));
		for (int i = 0; i < num_rules; i++) {
			char term;
			_index.read((char *)&term, sizeof(char));
			if (term) {
				ret->SetRule(i, NULL);
			} else {
				int len;
				_index.read((char *)&len, sizeof(int));
				int *vals = new int[len];
				_index.read((char *)vals, sizeof(int) * len);
				ret->SetRule(i, new Array((unsigned int*)vals, len));
				delete [] vals;
			}
		}
		ret->Compact();
		_index.close();
		return ret;
	}

protected:
	Array **rules_;
	Array *rules_seq_, *limits_;
	int num_rules_, dummy_term_, initial_;
	BitString *term_;
	BitSequenceRG *term_rs_;
};

class GrammarIndex {
public:
	GrammarIndex(const string &collection, const string &lists, const string &suff_sorted) {
		collection_ = Grammar::ReadPlain(collection);
		invlists_ = Grammar::ReadPlain(lists);

		suff_sorted_ = NULL;
		ifstream _offperm(suff_sorted.c_str());
		int len;
		_offperm.read((char *)&len, sizeof(int));
		int *data = new int[len];
		int *off = new int[len];
		for (int i = 0; i < len; i++) {
			_offperm.read((char *)&data[i], sizeof(int));
			_offperm.read((char *)&off[i], sizeof(int));
		}
		suff_sorted_ = new Array((unsigned int*)data, len);
		suff_offsets_ = new Array((unsigned int*)off, len);
		delete [] data;
		delete [] off;
		_offperm.close();
		
		// Create binary relation
		len = suff_sorted_->getLength();
		data = new int[len];
		for (int i = 0; i < len; i++) {
			data[i] = collection_->GetRule(suff_sorted_->getField(i), suff_offsets_->getField(i) - 1);
		}
		Array *binrel = new Array((unsigned int*)data, len);
		delete [] data;
		binrel_ = new WaveletTreeNoptrs(*binrel, new BitSequenceBuilderRG(20), new MapperNone());
		delete binrel;
		
		// Array *binrel = suff_sorted_;
		// binrel_ = new WaveletTreeNoptrs(*binrel, new BitSequenceBuilderRG(20), new MapperNone());
		
	}

	GrammarIndex(const string &fname) {
		ifstream in(fname.c_str());
		binrel_ = WaveletTreeNoptrs::load(in);
		suff_sorted_ = new Array(in);
		suff_offsets_ = new Array(in);
		collection_ = new Grammar(in);
		invlists_ = new Grammar(in);
		in.close();
	}

	~GrammarIndex() {
		delete binrel_;
		delete suff_offsets_;
		delete suff_sorted_;
		delete collection_;
		delete invlists_;
	}

	void Save(const string &fname) {
		ofstream out(fname.c_str());
		binrel_->save(out);
		suff_sorted_->save(out);
		suff_offsets_->save(out);
		collection_->Save(out);
		invlists_->Save(out);
		out.close();
	}

	int GetSize() {
		int size = binrel_->getSize();
		size += suff_sorted_->getSize();
		size += suff_offsets_->getSize();
		size += collection_->GetSize();
		size += invlists_->GetSize();
		return size;
	}

	int GetLength() {
		return collection_->GetExpandedLength(collection_->Initial());
	}

	void PrintStats() {
		cout << "INDEX STATISTICS" << endl << endl;
		cout << "* Collection: " << endl;
		collection_->PrintStats();
		cout << endl;
		cout << "* Inverted lists: " << endl;
		invlists_->PrintStats();
		cout << endl;
		cout << "* SuffSorted Permutation : " << suff_sorted_->getSize() << endl;
		cout << "* SuffOffsets Sequence   : " << suff_offsets_->getSize() << endl;
		cout << "* Binary relation        : " << binrel_->getSize() << endl;
	}

	int RangeRevLess(const uint *pattern, int len, int phrase) {
		stack<int> q;
		q.push(phrase);

		// int offset = collection_->Offset();
		int pos_phrase = 0;
		while (!q.empty()) {
			int r = q.top();
			q.pop();
			if (collection_->IsTerminal(r)) {
				int symb = collection_->GetSymbol(r);
				if (symb == pattern[len - pos_phrase - 1]) {
					if (pos_phrase == len - 1) return 0;
				} else if (symb < pattern[len - pos_phrase - 1]) {
					return 1;
				} else {
					return 0;
				}
				pos_phrase++;
			} else {
				int lenr = collection_->GetRuleLength(r);
				for (int i = 0; i < lenr; i++)
					q.push(collection_->GetRule(r, i));
			}
		}
		return 1;
	}

	int RangeRevLessEq(const uint *pattern, int len, int phrase) {
		stack<int> q;
		q.push(phrase);

		int pos_phrase = 0;
		while (!q.empty()) {
			int r = q.top();
			q.pop();
			if (collection_->IsTerminal(r)) {
				int symb = collection_->GetSymbol(r);
				if (symb == pattern[len - pos_phrase - 1]) {
					if (pos_phrase == len - 1) return 1;
				} else if (symb < pattern[len - pos_phrase - 1]) {
					return 1;
				} else {
					return 0;
				}
				pos_phrase++;
			} else {
				int lenr = collection_->GetRuleLength(r);
				for (int i = 0; i < lenr; i++)
					q.push(collection_->GetRule(r, i));
			}
		}
		return 1;
	}

	pair<int, int> RangeRevBin(const uint *pattern, int len) {
		if (len == 0) {
			return make_pair(0, (int)collection_->NumRules() - 1);
		}
		int count, step;
		int ini1 = 0, fin1 = collection_->NumRules();
		count = fin1 - ini1;

		while (count > 0) {
			step = count / 2;
			int pos = ini1 + step;
			if (RangeRevLess(pattern, len, pos)) {
				ini1 = pos + 1;
				count -= step + 1;
			} else {
				count = step;
			}
		}

		int ini2 = ini1, fin2 = collection_->NumRules();
		count = fin2 - ini2;
		while (count > 0) {
			step = count / 2;
			int pos = ini2 + step;
			if (RangeRevLessEq(pattern, len, pos)) {
				ini2 = pos + 1;
				count -= step + 1;
			} else {
				count = step;
			}
		}

		return make_pair(ini1, ini2-1);
	}

	pair<int, int> RangeSuffBin(const uint *pattern, int len) {
		if (len == 0) {
			return make_pair(0, (int)suff_sorted_->getLength() - 1);
		}
		int count, step;
		int ini1 = 0, fin1 = suff_sorted_->getLength();
		count = fin1 - ini1;

		while (count > 0) {
			step = count / 2;
			int pos = ini1 + step;
			if (RangeSuffLess(pattern, len, pos)) {
				ini1 = pos + 1;
				count -= step + 1;
			} else {
				count = step;
			}
		}

		int ini2 = ini1, fin2 = suff_sorted_->getLength();
		count = fin2 - ini2;
		while (count > 0) {
			step = count / 2;
			int pos = ini2 + step;
			if (RangeSuffLessEq(pattern, len, pos)) {
				ini2 = pos + 1;
				count -= step + 1;
			} else {
				count = step;
			}
		}
		
		return make_pair(ini1, ini2-1);
	}

	int RangeSuffLess(const uint *pattern, int len, int pos) {
		int pos_phrase = 0;
		stack<int> q;
		int rule_len = collection_->GetRuleLength(suff_sorted_->getField(pos));
		int rule_off = suff_offsets_->getField(pos);
		for (int i = rule_len - 1; i >= rule_off; i--)
			q.push(collection_->GetRule(suff_sorted_->getField(pos), i));

		while (!q.empty()) {
			int r = q.top();
			q.pop();
			if (collection_->IsTerminal(r)) {
				int symb = collection_->GetSymbol(r);
				if (symb == pattern[pos_phrase]) {
					if (pos_phrase == len - 1) return 0;
				} else if (symb < pattern[pos_phrase]) {
					return 1;
				} else {
					return 0;
				}
				pos_phrase++;
			} else {
				int lenr = collection_->GetRuleLength(r);
				for (int i = lenr-1; i >= 0; i--)
					q.push(collection_->GetRule(r, i));
			}
		}
		return 1;
	}

	int RangeSuffLessEq(const uint *pattern, int len, int pos) {
		int pos_phrase = 0;
		stack<int> q;
		int rule_len = collection_->GetRuleLength(suff_sorted_->getField(pos));
		int rule_off = suff_offsets_->getField(pos);
		for (int i = rule_len - 1; i >= rule_off; i--)
			q.push(collection_->GetRule(suff_sorted_->getField(pos), i));

		while (!q.empty()) {
			int r = q.top();
			q.pop();
			if (collection_->IsTerminal(r)) {
				int symb = collection_->GetSymbol(r);
				if (symb == pattern[pos_phrase]) {
					if (pos_phrase == len - 1) return 1;
				} else if (symb < pattern[pos_phrase]) {
					return 1;
				} else {
					return 0;
				}
				pos_phrase++;
			} else {
				int lenr = collection_->GetRuleLength(r);
				for (int i = lenr-1; i >= 0; i--)
					q.push(collection_->GetRule(r, i));
			}
		}
		return 1;
	}

	void RangeSearch(int rev_fst, int rev_lst, int suff_fst, int suff_lst, set<int> &res) {
		for (int i = suff_fst; i <= suff_lst; i++) {
			int v = (int)binrel_->access(i);
			if (v >= rev_fst && v <= rev_lst)
				res.insert(i);
		}
	}

	vector<int> *List(const uint *p, uint len) {
		#ifdef SHOW_STATS
		set_hits_ = 0;
		rules_hits_ = 0;
		#endif
		vector<int> *ret = new vector<int>();
		vector<int> nonterminals;
		for (int i = 1; i < len; i++) {
			pair<int, int> rev = RangeRevBin(p, i);
			pair<int, int> suff = RangeSuffBin(p + i, len - i);
			if (rev.first >= 0 && rev.second >= 0) {
				if (suff.first >= 0 && suff.second >= 0) {
					binrel_->range(suff.first, suff.second, rev.first, rev.second, &nonterminals);
				}
			}
		}
		AddDocuments(nonterminals, ret);
		#ifdef SHOW_STATS
		cout << "\tNonterminals explored: " << nonterminals.size() << endl;
		cout << "\tResult set size: " << ret->size() << endl;
		cout << "\tRules visited: " << rules_hits_ << endl;
		cout << "\tNumber of hits per element in the resulting set (avg): " << ((float)rules_hits_)/ret->size() << endl;
		cout << "\tThis is the equivalent to visiting " << (rules_hits_) << " occurrences" << endl;
		#endif
		return ret;
	}

	void AddDocuments(int r, set<int> &seen, vector<int> *s) {
		#ifdef SHOW_STATS
		rules_hits_++;
		#endif
		if (invlists_->IsTerminal(r)) {
			int symb = invlists_->GetSymbol(r);
			#ifdef SHOW_STATS
			set_hits_++;
			#endif
			s->push_back(symb);
		} else {
			int len = invlists_->GetRuleLength(r);
			for (int j = len - 1; j >= 0; j--) {
				int candidate = invlists_->GetRule(r, j);
				bool c = seen.insert(candidate).second;
				if (c)
					AddDocuments(candidate, seen, s);
			}
		}
	}

	void AddDocuments(vector<int> &nonterminals, vector<int> *s) {
		int rule = invlists_->Initial();

		set<int> seen;
		for (vector<int>::iterator it = nonterminals.begin(); it != nonterminals.end(); it++) {
			AddDocuments(invlists_->GetRule(rule, suff_sorted_->getField(*it)), seen, s);
		}
	}

	void ExtractDocument(int docid, vector<int> &document) {
		int last_rule = collection_->Initial();
		assert(docid < collection_->GetRuleLength(last_rule));
		
		stack<int> q;
		q.push(collection_->GetRule(last_rule, docid));

		while (!q.empty()) {
			int r = q.top();
			q.pop();
			if (collection_->IsTerminal(r)) {
				document.push_back(collection_->GetSymbol(r));
			} else {
				int len = collection_->GetRuleLength(r);
				for (int j = len - 1; j >= 0; j--) 
					q.push(collection_->GetRule(r, j));
			}
		}
	}

	void RebuildLists(ofstream &out) {
		int last_rule = invlists_->Initial();
		int num_lists = invlists_->GetRuleLength(last_rule);
		
		int num_docs = NumDocuments();

		// cout << "Number of documents: " << num_docs << endl;
		// cout << "Number of lists: " << num_lists << endl;

		for (int list = 0; list < num_lists; list++) {
			vector<int> results;
			stack<int> q;
			q.push(invlists_->GetRule(last_rule, list));

			while (!q.empty()) {
				int r = q.top();
				q.pop();
				if (invlists_->IsTerminal(r)) {
					if (invlists_->ShouldIgnore(r)) {
						// cout << "List " << list << " has a symbol I should ignore" << endl;
						break;
					}
					results.push_back(invlists_->GetSymbol(r));
				} else {
					int len = invlists_->GetRuleLength(r);
					for (int j = len - 1; j >= 0; j--) 
						q.push(invlists_->GetRule(r, j));
				}
			}

			for (int j = 0; j < results.size(); j++) {
				out.write((char *)&results[j], sizeof(int));
			}
			out.write((char *)&num_docs, sizeof(int));
			num_docs++;
		}
	}

	int NumDocuments() {
		int last_rule = collection_->Initial();
		return collection_->GetRuleLength(last_rule);
	}

protected:
	WaveletTreeNoptrs *binrel_;
	Array *suff_sorted_;
	Array *suff_offsets_;
	Grammar *collection_;
	Grammar *invlists_;
	#ifdef SHOW_STATS
	int rules_hits_;
	int set_hits_;
	#endif
};
