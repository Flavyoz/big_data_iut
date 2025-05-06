from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviePerTag(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_tag_movies,
                   reducer=self.reducer_count_tag_movies)
        ]

    def mapper_get_tag_movies(self, _, line):
        try:
            userID, movieID, tag, timestamp = line.split(',')
            yield movieID, 1
        except Exception:
            pass

    def reducer_count_tag_movies(self, tag, values):
        yield tag, sum(values)

if __name__ == '__main__':
    TagCounts.run()