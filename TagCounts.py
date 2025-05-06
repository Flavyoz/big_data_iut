from mrjob.job import MRJob
from mrjob.step import MRStep
import html

class TagCounts(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_tags(self, _, line):
        try:
            userID, movieID, tag, timestamp = line.split(',')
            tag = html.unescape(tag)
            yield movieID, 1
        except Exception:
            pass

    def reducer_count_tags(self, movieID, counts):
        yield movieID, sum(counts)

if __name__ == '__main__':
    TagCounts.run()
