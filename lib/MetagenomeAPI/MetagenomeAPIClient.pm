package MetagenomeAPI::MetagenomeAPIClient;

use JSON::RPC::Client;
use POSIX;
use strict;
use Data::Dumper;
use URI;
use Bio::KBase::Exceptions;
my $get_time = sub { time, 0 };
eval {
    require Time::HiRes;
    $get_time = sub { Time::HiRes::gettimeofday() };
};

use Bio::KBase::AuthToken;

# Client version should match Impl version
# This is a Semantic Version number,
# http://semver.org
our $VERSION = "0.1.0";

=head1 NAME

MetagenomeAPI::MetagenomeAPIClient

=head1 DESCRIPTION





=cut

sub new
{
    my($class, $url, @args) = @_;
    

    my $self = {
	client => MetagenomeAPI::MetagenomeAPIClient::RpcClient->new,
	url => $url,
	headers => [],
    };

    chomp($self->{hostname} = `hostname`);
    $self->{hostname} ||= 'unknown-host';

    #
    # Set up for propagating KBRPC_TAG and KBRPC_METADATA environment variables through
    # to invoked services. If these values are not set, we create a new tag
    # and a metadata field with basic information about the invoking script.
    #
    if ($ENV{KBRPC_TAG})
    {
	$self->{kbrpc_tag} = $ENV{KBRPC_TAG};
    }
    else
    {
	my ($t, $us) = &$get_time();
	$us = sprintf("%06d", $us);
	my $ts = strftime("%Y-%m-%dT%H:%M:%S.${us}Z", gmtime $t);
	$self->{kbrpc_tag} = "C:$0:$self->{hostname}:$$:$ts";
    }
    push(@{$self->{headers}}, 'Kbrpc-Tag', $self->{kbrpc_tag});

    if ($ENV{KBRPC_METADATA})
    {
	$self->{kbrpc_metadata} = $ENV{KBRPC_METADATA};
	push(@{$self->{headers}}, 'Kbrpc-Metadata', $self->{kbrpc_metadata});
    }

    if ($ENV{KBRPC_ERROR_DEST})
    {
	$self->{kbrpc_error_dest} = $ENV{KBRPC_ERROR_DEST};
	push(@{$self->{headers}}, 'Kbrpc-Errordest', $self->{kbrpc_error_dest});
    }

    #
    # This module requires authentication.
    #
    # We create an auth token, passing through the arguments that we were (hopefully) given.

    {
	my %arg_hash2 = @args;
	if (exists $arg_hash2{"token"}) {
	    $self->{token} = $arg_hash2{"token"};
	} elsif (exists $arg_hash2{"user_id"}) {
	    my $token = Bio::KBase::AuthToken->new(@args);
	    if (!$token->error_message) {
	        $self->{token} = $token->token;
	    }
	}
	
	if (exists $self->{token})
	{
	    $self->{client}->{token} = $self->{token};
	}
    }

    my $ua = $self->{client}->ua;	 
    my $timeout = $ENV{CDMI_TIMEOUT} || (30 * 60);	 
    $ua->timeout($timeout);
    bless $self, $class;
    #    $self->_validate_version();
    return $self;
}




=head2 search_binned_contigs

  $result = $obj->search_binned_contigs($params)

=over 4

=item Parameter and return types

=begin html

<pre>
$params is a MetagenomeAPI.SearchBinnedContigsOptions
$result is a MetagenomeAPI.SearchBinnedContigsResult
SearchBinnedContigsOptions is a reference to a hash where the following keys are defined:
	ref has a value which is a string
	query has a value which is a string
	sort_by has a value which is a reference to a list where each element is a MetagenomeAPI.column_sorting
	start has a value which is an int
	limit has a value which is an int
	num_found has a value which is an int
column_sorting is a reference to a list containing 2 items:
	0: (column) a string
	1: (ascending) a MetagenomeAPI.boolean
boolean is an int
SearchBinnedContigsResult is a reference to a hash where the following keys are defined:
	query has a value which is a string
	start has a value which is an int
	bins has a value which is a reference to a list where each element is a MetagenomeAPI.ContigBinData
	num_found has a value which is an int
ContigBinData is a reference to a hash where the following keys are defined:
	bin_id has a value which is a string
	n_contigs has a value which is an int
	gc has a value which is a float
	sum_contig_len has a value which is an int
	cov has a value which is a float

</pre>

=end html

=begin text

$params is a MetagenomeAPI.SearchBinnedContigsOptions
$result is a MetagenomeAPI.SearchBinnedContigsResult
SearchBinnedContigsOptions is a reference to a hash where the following keys are defined:
	ref has a value which is a string
	query has a value which is a string
	sort_by has a value which is a reference to a list where each element is a MetagenomeAPI.column_sorting
	start has a value which is an int
	limit has a value which is an int
	num_found has a value which is an int
column_sorting is a reference to a list containing 2 items:
	0: (column) a string
	1: (ascending) a MetagenomeAPI.boolean
boolean is an int
SearchBinnedContigsResult is a reference to a hash where the following keys are defined:
	query has a value which is a string
	start has a value which is an int
	bins has a value which is a reference to a list where each element is a MetagenomeAPI.ContigBinData
	num_found has a value which is an int
ContigBinData is a reference to a hash where the following keys are defined:
	bin_id has a value which is a string
	n_contigs has a value which is an int
	gc has a value which is a float
	sum_contig_len has a value which is an int
	cov has a value which is a float


=end text

=item Description



=back

=cut

 sub search_binned_contigs
{
    my($self, @args) = @_;

# Authentication: optional

    if ((my $n = @args) != 1)
    {
	Bio::KBase::Exceptions::ArgumentValidationError->throw(error =>
							       "Invalid argument count for function search_binned_contigs (received $n, expecting 1)");
    }
    {
	my($params) = @args;

	my @_bad_arguments;
        (ref($params) eq 'HASH') or push(@_bad_arguments, "Invalid type for argument 1 \"params\" (value was \"$params\")");
        if (@_bad_arguments) {
	    my $msg = "Invalid arguments passed to search_binned_contigs:\n" . join("", map { "\t$_\n" } @_bad_arguments);
	    Bio::KBase::Exceptions::ArgumentValidationError->throw(error => $msg,
								   method_name => 'search_binned_contigs');
	}
    }

    my $url = $self->{url};
    my $result = $self->{client}->call($url, $self->{headers}, {
	    method => "MetagenomeAPI.search_binned_contigs",
	    params => \@args,
    });
    if ($result) {
	if ($result->is_error) {
	    Bio::KBase::Exceptions::JSONRPC->throw(error => $result->error_message,
					       code => $result->content->{error}->{code},
					       method_name => 'search_binned_contigs',
					       data => $result->content->{error}->{error} # JSON::RPC::ReturnObject only supports JSONRPC 1.1 or 1.O
					      );
	} else {
	    return wantarray ? @{$result->result} : $result->result->[0];
	}
    } else {
        Bio::KBase::Exceptions::HTTP->throw(error => "Error invoking method search_binned_contigs",
					    status_line => $self->{client}->status_line,
					    method_name => 'search_binned_contigs',
				       );
    }
}
 


=head2 search_contigs_in_bin

  $result = $obj->search_contigs_in_bin($params)

=over 4

=item Parameter and return types

=begin html

<pre>
$params is a MetagenomeAPI.SearchContigsInBin
$result is a MetagenomeAPI.SearchContigsInBinResult
SearchContigsInBin is a reference to a hash where the following keys are defined:
	ref has a value which is a string
	bin_id has a value which is a string
	query has a value which is a string
	sort_by has a value which is a reference to a list where each element is a MetagenomeAPI.column_sorting
	start has a value which is an int
	limit has a value which is an int
	num_found has a value which is an int
column_sorting is a reference to a list containing 2 items:
	0: (column) a string
	1: (ascending) a MetagenomeAPI.boolean
boolean is an int
SearchContigsInBinResult is a reference to a hash where the following keys are defined:
	query has a value which is a string
	bin_id has a value which is a string
	start has a value which is an int
	contigs has a value which is a reference to a list where each element is a MetagenomeAPI.ContigInBin
	num_found has a value which is an int
ContigInBin is a reference to a hash where the following keys are defined:
	contig_id has a value which is a string
	len has a value which is an int
	gc has a value which is a float
	cov has a value which is a float

</pre>

=end html

=begin text

$params is a MetagenomeAPI.SearchContigsInBin
$result is a MetagenomeAPI.SearchContigsInBinResult
SearchContigsInBin is a reference to a hash where the following keys are defined:
	ref has a value which is a string
	bin_id has a value which is a string
	query has a value which is a string
	sort_by has a value which is a reference to a list where each element is a MetagenomeAPI.column_sorting
	start has a value which is an int
	limit has a value which is an int
	num_found has a value which is an int
column_sorting is a reference to a list containing 2 items:
	0: (column) a string
	1: (ascending) a MetagenomeAPI.boolean
boolean is an int
SearchContigsInBinResult is a reference to a hash where the following keys are defined:
	query has a value which is a string
	bin_id has a value which is a string
	start has a value which is an int
	contigs has a value which is a reference to a list where each element is a MetagenomeAPI.ContigInBin
	num_found has a value which is an int
ContigInBin is a reference to a hash where the following keys are defined:
	contig_id has a value which is a string
	len has a value which is an int
	gc has a value which is a float
	cov has a value which is a float


=end text

=item Description



=back

=cut

 sub search_contigs_in_bin
{
    my($self, @args) = @_;

# Authentication: optional

    if ((my $n = @args) != 1)
    {
	Bio::KBase::Exceptions::ArgumentValidationError->throw(error =>
							       "Invalid argument count for function search_contigs_in_bin (received $n, expecting 1)");
    }
    {
	my($params) = @args;

	my @_bad_arguments;
        (ref($params) eq 'HASH') or push(@_bad_arguments, "Invalid type for argument 1 \"params\" (value was \"$params\")");
        if (@_bad_arguments) {
	    my $msg = "Invalid arguments passed to search_contigs_in_bin:\n" . join("", map { "\t$_\n" } @_bad_arguments);
	    Bio::KBase::Exceptions::ArgumentValidationError->throw(error => $msg,
								   method_name => 'search_contigs_in_bin');
	}
    }

    my $url = $self->{url};
    my $result = $self->{client}->call($url, $self->{headers}, {
	    method => "MetagenomeAPI.search_contigs_in_bin",
	    params => \@args,
    });
    if ($result) {
	if ($result->is_error) {
	    Bio::KBase::Exceptions::JSONRPC->throw(error => $result->error_message,
					       code => $result->content->{error}->{code},
					       method_name => 'search_contigs_in_bin',
					       data => $result->content->{error}->{error} # JSON::RPC::ReturnObject only supports JSONRPC 1.1 or 1.O
					      );
	} else {
	    return wantarray ? @{$result->result} : $result->result->[0];
	}
    } else {
        Bio::KBase::Exceptions::HTTP->throw(error => "Error invoking method search_contigs_in_bin",
					    status_line => $self->{client}->status_line,
					    method_name => 'search_contigs_in_bin',
				       );
    }
}
 


=head2 get_annotated_metagenome_assembly

  $output = $obj->get_annotated_metagenome_assembly($params)

=over 4

=item Parameter and return types

=begin html

<pre>
$params is a MetagenomeAPI.getAnnotatedMetagenomeAssemblyParams
$output is a MetagenomeAPI.getAnnotatedMetagenomeAssemblyOutput
getAnnotatedMetagenomeAssemblyParams is a reference to a hash where the following keys are defined:
	ref has a value which is a string
	included_fields has a value which is a reference to a list where each element is a string
getAnnotatedMetagenomeAssemblyOutput is a reference to a hash where the following keys are defined:
	genomes has a value which is a reference to a list where each element is an UnspecifiedObject, which can hold any non-null object

</pre>

=end html

=begin text

$params is a MetagenomeAPI.getAnnotatedMetagenomeAssemblyParams
$output is a MetagenomeAPI.getAnnotatedMetagenomeAssemblyOutput
getAnnotatedMetagenomeAssemblyParams is a reference to a hash where the following keys are defined:
	ref has a value which is a string
	included_fields has a value which is a reference to a list where each element is a string
getAnnotatedMetagenomeAssemblyOutput is a reference to a hash where the following keys are defined:
	genomes has a value which is a reference to a list where each element is an UnspecifiedObject, which can hold any non-null object


=end text

=item Description



=back

=cut

 sub get_annotated_metagenome_assembly
{
    my($self, @args) = @_;

# Authentication: required

    if ((my $n = @args) != 1)
    {
	Bio::KBase::Exceptions::ArgumentValidationError->throw(error =>
							       "Invalid argument count for function get_annotated_metagenome_assembly (received $n, expecting 1)");
    }
    {
	my($params) = @args;

	my @_bad_arguments;
        (ref($params) eq 'HASH') or push(@_bad_arguments, "Invalid type for argument 1 \"params\" (value was \"$params\")");
        if (@_bad_arguments) {
	    my $msg = "Invalid arguments passed to get_annotated_metagenome_assembly:\n" . join("", map { "\t$_\n" } @_bad_arguments);
	    Bio::KBase::Exceptions::ArgumentValidationError->throw(error => $msg,
								   method_name => 'get_annotated_metagenome_assembly');
	}
    }

    my $url = $self->{url};
    my $result = $self->{client}->call($url, $self->{headers}, {
	    method => "MetagenomeAPI.get_annotated_metagenome_assembly",
	    params => \@args,
    });
    if ($result) {
	if ($result->is_error) {
	    Bio::KBase::Exceptions::JSONRPC->throw(error => $result->error_message,
					       code => $result->content->{error}->{code},
					       method_name => 'get_annotated_metagenome_assembly',
					       data => $result->content->{error}->{error} # JSON::RPC::ReturnObject only supports JSONRPC 1.1 or 1.O
					      );
	} else {
	    return wantarray ? @{$result->result} : $result->result->[0];
	}
    } else {
        Bio::KBase::Exceptions::HTTP->throw(error => "Error invoking method get_annotated_metagenome_assembly",
					    status_line => $self->{client}->status_line,
					    method_name => 'get_annotated_metagenome_assembly',
				       );
    }
}
 
  
sub status
{
    my($self, @args) = @_;
    if ((my $n = @args) != 0) {
        Bio::KBase::Exceptions::ArgumentValidationError->throw(error =>
                                   "Invalid argument count for function status (received $n, expecting 0)");
    }
    my $url = $self->{url};
    my $result = $self->{client}->call($url, $self->{headers}, {
        method => "MetagenomeAPI.status",
        params => \@args,
    });
    if ($result) {
        if ($result->is_error) {
            Bio::KBase::Exceptions::JSONRPC->throw(error => $result->error_message,
                           code => $result->content->{error}->{code},
                           method_name => 'status',
                           data => $result->content->{error}->{error} # JSON::RPC::ReturnObject only supports JSONRPC 1.1 or 1.O
                          );
        } else {
            return wantarray ? @{$result->result} : $result->result->[0];
        }
    } else {
        Bio::KBase::Exceptions::HTTP->throw(error => "Error invoking method status",
                        status_line => $self->{client}->status_line,
                        method_name => 'status',
                       );
    }
}
   

sub version {
    my ($self) = @_;
    my $result = $self->{client}->call($self->{url}, $self->{headers}, {
        method => "MetagenomeAPI.version",
        params => [],
    });
    if ($result) {
        if ($result->is_error) {
            Bio::KBase::Exceptions::JSONRPC->throw(
                error => $result->error_message,
                code => $result->content->{code},
                method_name => 'get_annotated_metagenome_assembly',
            );
        } else {
            return wantarray ? @{$result->result} : $result->result->[0];
        }
    } else {
        Bio::KBase::Exceptions::HTTP->throw(
            error => "Error invoking method get_annotated_metagenome_assembly",
            status_line => $self->{client}->status_line,
            method_name => 'get_annotated_metagenome_assembly',
        );
    }
}

sub _validate_version {
    my ($self) = @_;
    my $svr_version = $self->version();
    my $client_version = $VERSION;
    my ($cMajor, $cMinor) = split(/\./, $client_version);
    my ($sMajor, $sMinor) = split(/\./, $svr_version);
    if ($sMajor != $cMajor) {
        Bio::KBase::Exceptions::ClientServerIncompatible->throw(
            error => "Major version numbers differ.",
            server_version => $svr_version,
            client_version => $client_version
        );
    }
    if ($sMinor < $cMinor) {
        Bio::KBase::Exceptions::ClientServerIncompatible->throw(
            error => "Client minor version greater than Server minor version.",
            server_version => $svr_version,
            client_version => $client_version
        );
    }
    if ($sMinor > $cMinor) {
        warn "New client version available for MetagenomeAPI::MetagenomeAPIClient\n";
    }
    if ($sMajor == 0) {
        warn "MetagenomeAPI::MetagenomeAPIClient version is $svr_version. API subject to change.\n";
    }
}

=head1 TYPES



=head2 boolean

=over 4



=item Description

Indicates true or false values, false = 0, true = 1
@range [0,1]


=item Definition

=begin html

<pre>
an int
</pre>

=end html

=begin text

an int

=end text

=back



=head2 column_sorting

=over 4



=item Definition

=begin html

<pre>
a reference to a list containing 2 items:
0: (column) a string
1: (ascending) a MetagenomeAPI.boolean

</pre>

=end html

=begin text

a reference to a list containing 2 items:
0: (column) a string
1: (ascending) a MetagenomeAPI.boolean


=end text

=back



=head2 SearchBinnedContigsOptions

=over 4



=item Description

num_found - optional field which when set informs that there
    is no need to perform full scan in order to count this
    value because it was already done before; please don't
    set this value with 0 or any guessed number if you didn't 
    get right value previously.


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
ref has a value which is a string
query has a value which is a string
sort_by has a value which is a reference to a list where each element is a MetagenomeAPI.column_sorting
start has a value which is an int
limit has a value which is an int
num_found has a value which is an int

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
ref has a value which is a string
query has a value which is a string
sort_by has a value which is a reference to a list where each element is a MetagenomeAPI.column_sorting
start has a value which is an int
limit has a value which is an int
num_found has a value which is an int


=end text

=back



=head2 ContigBinData

=over 4



=item Description

bin_id          - id of the bin
n_contigs       - number of contigs in this bin
gc              - GC content over all the contigs
sum_contig_len  - (bp) total length of the contigs
cov             - coverage over the bin (if available, may be null)


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
bin_id has a value which is a string
n_contigs has a value which is an int
gc has a value which is a float
sum_contig_len has a value which is an int
cov has a value which is a float

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
bin_id has a value which is a string
n_contigs has a value which is an int
gc has a value which is a float
sum_contig_len has a value which is an int
cov has a value which is a float


=end text

=back



=head2 SearchBinnedContigsResult

=over 4



=item Description

num_found - number of all items found in query search (with 
    only part of it returned in "bins" list).


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
query has a value which is a string
start has a value which is an int
bins has a value which is a reference to a list where each element is a MetagenomeAPI.ContigBinData
num_found has a value which is an int

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
query has a value which is a string
start has a value which is an int
bins has a value which is a reference to a list where each element is a MetagenomeAPI.ContigBinData
num_found has a value which is an int


=end text

=back



=head2 SearchContigsInBin

=over 4



=item Description

num_found - optional field which when set informs that there
    is no need to perform full scan in order to count this
    value because it was already done before; please don't
    set this value with 0 or any guessed number if you didn't 
    get right value previously.


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
ref has a value which is a string
bin_id has a value which is a string
query has a value which is a string
sort_by has a value which is a reference to a list where each element is a MetagenomeAPI.column_sorting
start has a value which is an int
limit has a value which is an int
num_found has a value which is an int

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
ref has a value which is a string
bin_id has a value which is a string
query has a value which is a string
sort_by has a value which is a reference to a list where each element is a MetagenomeAPI.column_sorting
start has a value which is an int
limit has a value which is an int
num_found has a value which is an int


=end text

=back



=head2 ContigInBin

=over 4



=item Description

contig_id       - id of the contig
len             - (bp) length of the contig
gc              - GC content over the contig
cov             - coverage over the contig (if available, may be null)


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
contig_id has a value which is a string
len has a value which is an int
gc has a value which is a float
cov has a value which is a float

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
contig_id has a value which is a string
len has a value which is an int
gc has a value which is a float
cov has a value which is a float


=end text

=back



=head2 SearchContigsInBinResult

=over 4



=item Description

num_found - number of all items found in query search (with 
    only part of it returned in "bins" list).


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
query has a value which is a string
bin_id has a value which is a string
start has a value which is an int
contigs has a value which is a reference to a list where each element is a MetagenomeAPI.ContigInBin
num_found has a value which is an int

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
query has a value which is a string
bin_id has a value which is a string
start has a value which is an int
contigs has a value which is a reference to a list where each element is a MetagenomeAPI.ContigInBin
num_found has a value which is an int


=end text

=back



=head2 getAnnotatedMetagenomeAssemblyParams

=over 4



=item Description

ref - workspace reference to AnnotatedMetagenomeAssembly Object
included_fields - The fields to include from the Object
included_feature_fields -


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
ref has a value which is a string
included_fields has a value which is a reference to a list where each element is a string

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
ref has a value which is a string
included_fields has a value which is a reference to a list where each element is a string


=end text

=back



=head2 getAnnotatedMetagenomeAssemblyOutput

=over 4



=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
genomes has a value which is a reference to a list where each element is an UnspecifiedObject, which can hold any non-null object

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
genomes has a value which is a reference to a list where each element is an UnspecifiedObject, which can hold any non-null object


=end text

=back



=cut

package MetagenomeAPI::MetagenomeAPIClient::RpcClient;
use base 'JSON::RPC::Client';
use POSIX;
use strict;

#
# Override JSON::RPC::Client::call because it doesn't handle error returns properly.
#

sub call {
    my ($self, $uri, $headers, $obj) = @_;
    my $result;


    {
	if ($uri =~ /\?/) {
	    $result = $self->_get($uri);
	}
	else {
	    Carp::croak "not hashref." unless (ref $obj eq 'HASH');
	    $result = $self->_post($uri, $headers, $obj);
	}

    }

    my $service = $obj->{method} =~ /^system\./ if ( $obj );

    $self->status_line($result->status_line);

    if ($result->is_success) {

        return unless($result->content); # notification?

        if ($service) {
            return JSON::RPC::ServiceObject->new($result, $self->json);
        }

        return JSON::RPC::ReturnObject->new($result, $self->json);
    }
    elsif ($result->content_type eq 'application/json')
    {
        return JSON::RPC::ReturnObject->new($result, $self->json);
    }
    else {
        return;
    }
}


sub _post {
    my ($self, $uri, $headers, $obj) = @_;
    my $json = $self->json;

    $obj->{version} ||= $self->{version} || '1.1';

    if ($obj->{version} eq '1.0') {
        delete $obj->{version};
        if (exists $obj->{id}) {
            $self->id($obj->{id}) if ($obj->{id}); # if undef, it is notification.
        }
        else {
            $obj->{id} = $self->id || ($self->id('JSON::RPC::Client'));
        }
    }
    else {
        # $obj->{id} = $self->id if (defined $self->id);
	# Assign a random number to the id if one hasn't been set
	$obj->{id} = (defined $self->id) ? $self->id : substr(rand(),2);
    }

    my $content = $json->encode($obj);

    $self->ua->post(
        $uri,
        Content_Type   => $self->{content_type},
        Content        => $content,
        Accept         => 'application/json',
	@$headers,
	($self->{token} ? (Authorization => $self->{token}) : ()),
    );
}



1;
