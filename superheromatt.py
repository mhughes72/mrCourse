from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):

    def configure_args(self):
        super(MostPopularMovie, self).configure_args()
        self.add_file_arg('--hero', help='Path to Marvel-Names.txt')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_friends,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer_combine_friends),
            MRStep(mapper = self.mapper_passthrough,
                   reducer = self.reducer_find_max)
        ]

    def mapper_get_friends(self, _, line):
        heroAndFriends = line.split()
        hero=heroAndFriends[0]
        numFriends=len(heroAndFriends)-1
        yield hero, numFriends

    def reducer_init(self):
        self.heroNames = {}
        with open("Marvel-Names.txt", encoding='ascii', errors='ignore') as f:
            for line in f:
                fields = line.split()
                self.heroNames[fields[0]] = fields[1]

    def reducer_combine_friends(self, key, values):
        yield key, sum(values)

#This mapper does nothing; it's just here to avoid a bug in some
#versions of mrjob related to "non-script steps." Normally this
#wouldn't be needed.
    def mapper_passthrough(self, key, value):
        yield key, value

    def reducer_find_max(self, key, values):
        yield max(values)

if __name__ == '__main__':
    MostPopularMovie.run()
