require 'rubygems'
require 'rev'
require File.dirname(__FILE__) + '/../lib/rev_memcache'

event_loop = Rev::Loop.default
cache = Rev::Memcache.connect('localhost', 11211).attach(event_loop)

cache.set :a, 'hello'
cache.set :b, 'hi'
cache.set :c, 'how are you?'
cache.set :d, ''

cache.get(:a){ |v| p v }
cache.get_hash(:a, :b, :c, :d){ |v| p v }
cache.get(:a,:b,:c,:d){ |a,b,c,d| p [a,b,c,d] }

cache.get(:a,:z,:b,:y,:d){ |a,z,b,y,d| p [a,z,b,y,d] }

cache.get(:missing){ |m| p [:missing=, m] }
cache.set(:missing, 'abc'){ p :stored }
cache.get(:missing){ |m| p [:missing=, m] }
cache.del(:missing){ p :deleted }
cache.get(:missing){ |m| p [:missing=, m] }

event_loop.run