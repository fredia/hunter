import sys

sys.path.append(r"/Users/leiyanfei/code/hunter")
from hunter.test_config import CsvMetric
from hunter.series import AnalysisOptions, Series
from hunter.main import compare


def checkWithHunter(i, time, data, commits, regressionSet):
    tp, fp, tn, fn = 0, 0, 0, 0
    metric = CsvMetric("metric1", 1, 1.0, "metric1")
    series = Series(str(i), branch=None, time=time[:i],
                    metrics={'metric1': metric}, data={'metric1': data[:i]}, attributes={'commit': commits[:i]})
    analyzed_series = series.analyze(AnalysisOptions())
    change_points = analyzed_series.change_points_by_time
    if len(change_points) > 0 and change_points[-1].changes[-1].forward_change_percent() < 0:
        cmp = compare(analyzed_series, change_points[-1].index - 1, analyzed_series, i)
        stats = cmp.stats['metric1']
        m1 = stats.mean_1
        m2 = stats.mean_2
        change_percent = stats.forward_rel_change() * 100.0
        if m2 < m1 and stats.pvalue < 0.001:  # postive
            if commits[i] in regressionSet:  # true postive
                tp += 1
            else:  # false postive
                fp += 1
    if tp + fp == 0:
        if commits[i] in regressionSet:  # false negative
            fn += 1
        else:  # true negative
            tn += 1
    return fn, fp, tn, tp


def getMedian(lst):
    lst = sorted(lst)  # Sort the list first
    if len(lst) % 2 == 0:  # Checking if the length is even
        # Applying formula which is sum of middle two divided by 2
        return (lst[len(lst) // 2] + lst[(len(lst) - 1) // 2]) / 2
    else:
        # If length is odd then get middle value
        return lst[len(lst) // 2]


def isThresholdReached(threshold, baselineValue, comparedValue, lessIsbBetter):
    ratio = 0
    if baselineValue != 0:
        ratio = comparedValue * 100 / baselineValue - 100
    # regression detected
    if lessIsbBetter:
        if ratio > (-1 * threshold):
            return True
    elif ratio < threshold:
        return True

    return False


def checkWithMedian(regressionSet, scores, index, commit):
    fn = 0
    fp = 0
    tn = 0
    tp = 0
    median = getMedian(scores[index - 100: index - 20])
    recent_median = getMedian(scores[index - 20: index])
    if isThresholdReached(-4, median, recent_median, False):  # postive
        if commit in regressionSet:  # true postive
            tp += 1
        else:  # false postive
            fp += 1
    else:  # negative
        if commit in regressionSet:  # false negative
            fn += 1
        else:  # true negative
            tn += 1
    return fn, fp, tn, tp


def checkWithMax(regressionSet, stds, scores, index, commit):
    fn = 0
    fp = 0
    tn = 0
    tp = 0
    sustainable_x = [min(scores[i - 2: i + 1]) for i in range(index - 90, index)]
    baseline_throughput = max(sustainable_x)
    current_throughput = max(scores[index - 3: index])
    current_unstability = stds[index] / current_throughput

    if 1 - current_throughput / baseline_throughput > max(0.04, 2 * current_unstability):  # postive
        if commit in regressionSet:  # true postive
            tp += 1
        else:  # false postive
            fp += 1
    else:  # negative
        if commit in regressionSet:  # false negative
            fn += 1
        else:  # true negative
            tn += 1

    return fn, fp, tn, tp


def testRegression(exeName, benchmark, commitStart, commitEnd, methodName="Median"):
    time = []
    data = []
    commits = []
    stds = []
    fileName = "tests/resources/" + benchmark + exeName + ".csv"
    with open(fileName, 'r', encoding="UTF-8") as f:
        f.readline()
        for row in f.readlines():
            row_data = row.strip().split(',')
            time.append(row_data[0])
            commits.append(row_data[1])
            stds.append(float(row_data[2]))
            data.append(float(row_data[3]))
    regressionStartIndex = -1
    regressionEndIndex = -1
    for i, commit in enumerate(commits):
        if commit == commitStart:
            if regressionStartIndex == -1:
                regressionStartIndex = i
        elif commit == commitEnd:
            regressionEndIndex = i
    regressionSet = set(commits[regressionStartIndex: regressionEndIndex + 1])
    # print("Regression range:", regressionStartIndex, regressionEndIndex)
    fn = 0
    fp = 0
    tn = 0
    tp = 0
    # for each commit-point, check regression
    for i in range(100, len(data)):
        fn1, fp1, tn1, tp1 = 0, 0, 0, 0
        if methodName == "Hunter":
            fn1, fp1, tn1, tp1 = checkWithHunter(i, time, data, commits, regressionSet)
        elif methodName == "Median":
            fn1, fp1, tn1, tp1 = checkWithMedian(regressionSet, data, i, commits[i])
        elif methodName == "Max":
            fn1, fp1, tn1, tp1 = checkWithMax(regressionSet, stds, data, i, commits[i])
        fn += fn1
        fp += fp1
        tn += tn1
        tp += tp1
    print("true postive:", tp, "false postive:", fp, "true negative:", tn, "false negative:", fn)
    precision, recall, f1 = 0, 0, 0
    if tp + fp > 0:
        precision = tp * 1.0 / (tp + fp)
    if tp + fn > 0:
        recall = tp * 1.0 / (tp + fn)
    if precision + recall > 0:
        f1 = 2 * precision * recall / (precision + recall)
    print(benchmark, "precision:", precision, "recall:", recall, "f1:", f1)


if __name__ == "__main__":
    exe2num = {
        'Flink': '1',
        'Flink (Java11)': '6',
        'Scheduler': '5',
        'Scheduler (Java11)': '8',
        'State Backends': '3',
        'State Backends (Java11)': '9'
    }
    benchmarkWithRegression = [
        ('serializerRow', 'Flink (Java11)', '314e276f6c', 'f9ba9d10f5'),
        ('serializerRow', 'Flink', '314e276f6c', 'f9ba9d10f5'),
        ('checkpointSingleInput.UNALIGNED_1', 'Flink (Java11)', 'fb2722cdeb', '88cb0853b7'),
        ('checkpointSingleInput.UNALIGNED_1', 'Flink', 'fb2722cdeb', '88cb0853b7'),
        # partial fix: 88cb0853b7, latest commit 5a4e0ea31a
        ('arrayKeyBy', 'Flink', 'ff3336266e', 'ce37fc902b')]
    # same as serializerRow, no need to test
    # ('asyncWait.ORDERED', 'Flink', '314e276f6c','f9ba9d10f5'),
    # ('asyncWait.UNORDERED', 'Flink', '314e276f6c','f9ba9d10f5'),
    # ('sortedMultiInput', 'Flink', '314e276f6c','f9ba9d10f5'),
    # ('sortedOneInput', 'Flink', '314e276f6c','f9ba9d10f5'),
    # ('sortedTwoInput', 'Flink', '314e276f6c','f9ba9d10f5')]

    benchmarkWithNoisy = [('fireProcessingTimers', 'Flink (Java11)'),
                          ('fireProcessingTimers', 'Flink')]

    regressionWithNoisy = [('serializerTuple', 'Flink (Java11)', "314e276f6c", "f9ba9d10f5")]
    # test regression
    for bench in benchmarkWithRegression:
        print("###########" + bench[0] + "," + bench[1] + "###########")
        print("-------Hunter---------")
        testRegression(exe2num[bench[1]], bench[0], bench[2], bench[3], "Hunter")
        print("-------Max---------")
        testRegression(exe2num[bench[1]], bench[0], bench[2], bench[3], "Max")
        print("-------Median---------")
        testRegression(exe2num[bench[1]], bench[0], bench[2], bench[3], "Median")
        print("\n")

    # print("\n\n\ntest noisy")
    # for bench in benchmarkWithNoisy:
    #     print("###########" + bench[0] + "," + bench[1] + "###########")
    #     print("-------Hunter---------")
    #     testRegression(exe2num[bench[1]], bench[0], "", "", "Hunter")
    #     print("-------Max---------")
    #     testRegression(exe2num[bench[1]], bench[0], "", "", "Max")
    #     print("-------Median---------")
    #     testRegression(exe2num[bench[1]], bench[0], "", "", "Median")
    #     print("\n")

    # for bench in regressionWithNoisy:
    #     print("###########" + bench[0] + "," + bench[1] + "###########")
    #     print("-------Hunter---------")
    #     testRegression(exe2num[bench[1]], bench[0], bench[2], bench[3], "Hunter")
    #     print("-------Max---------")
    #     testRegression(exe2num[bench[1]], bench[0], bench[2], bench[3], "Max")
    #     print("-------Median---------")
    #     testRegression(exe2num[bench[1]], bench[0], bench[2], bench[3], "Median")
    #     print("\n")
