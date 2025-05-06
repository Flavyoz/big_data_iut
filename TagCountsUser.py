from mrjob.job import MRJob
from mrjob.step import MRStep

class TagCounts(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_tags_user,
                   reducer=self.reducer_count_tags_user)
        ]

    def mapper_get_tags_user(self, _, line):
        try:
            userID, movieID, tag, timestamp = line.split(',')
            yield userID, 1
        except Exception:
            pass

    def reducer_count_tags_user(self, userID, counts):
        yield userID, sum(counts)

if __name__ == '__main__':
    TagCounts.run()