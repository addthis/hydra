export default function numberComparator(a, b) {
	if (isNaN(a)) {
		if (isNaN(b)) {
			return 0;
		}
		else {
			return -1;
		}
	}
	else if (isNaN(b)) {
		return 1;
	}
	else {
		return Number(a) - Number(b);
	}
}
