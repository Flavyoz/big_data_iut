from mrjob.job import MRJob
from mrjob.step import MRStep

class TagCounts(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_tags(self, _, line):
        try:
            (userID, movieID, tag, timestamp) = line.split(',')
            yield tag, 1
        except Exception:
            pass

    def reducer_count_tags(self, tag, counts):
        yield tag, sum(counts)

if __name__ == '__main__':
    TagCounts.run()
