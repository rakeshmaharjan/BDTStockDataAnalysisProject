package main;

public class Stock {
	Data data;

	public Data getData() {
		return data;
	}

	public void setData(Data data) {
		this.data = data;
	}

	public class Data {
		String Index;
		String High;

		public Data(String Index, String High) {
			this.Index = Index;
			this.High = High;
		}

		public String getIndex() {
			return Index;
		}

		public void setIndex(String index) {
			this.Index = index;
		}

		public String getHigh() {
			return High;
		}

		public void setHigh(String high) {
			this.High = high;
		}

	}
}
